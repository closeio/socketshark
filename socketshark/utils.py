import asyncio
import ssl
import time
from urllib.parse import urlsplit

import aiohttp


from . import constants as c


def _get_rate_limit_wait(log, resp, opts):
    """
    Returns the number of seconds we should wait given a 429 HTTP response and
    HTTP options.
    """
    max_wait = 3600

    wait = opts['wait']

    header_name = opts['rate_limit_reset_header_name']
    if header_name and header_name in resp.headers:
        header_value = resp.headers[header_name]
        try:
            new_wait = float(header_value)
            # Make sure we have a valid value (not negative, NaN, or Inf)
            if 0 <= new_wait <= max_wait:
                wait = new_wait
            elif new_wait > max_wait:
                log.warn(
                    'rate reset value too high',
                    name=header_name,
                    value=header_value,
                )
                wait = max_wait
            else:
                log.warn(
                    'invalid rate reset value',
                    name=header_name,
                    value=header_value,
                )
        except ValueError:
            log.warn(
                'invalid rate reset value',
                name=header_name,
                value=header_value,
            )
    return wait


def _scrub_url(url):
    """Scrub URL username and password."""
    url_parts = urlsplit(url)
    if url_parts.password is None:
        return url
    else:
        # url_parts tuple doesn't include password in _fields
        # so can't easily use _replace to get rid of password
        # and then call urlunsplit to reconstruct url.
        if url_parts.port is None:
            port = ''
        else:
            port = f':{url_parts.port}'
        return (
            f'{url_parts.scheme}://*****:*****@{url_parts.hostname}'
            f'{port}{url_parts.path}?{url_parts.query}'
        )


async def http_post(shark, url, data):
    log = shark.log.bind(url=_scrub_url(url))
    opts = shark.config['HTTP']
    if opts.get('ssl_cafile'):
        ssl_context = ssl.create_default_context(cafile=opts['ssl_cafile'])
    else:
        ssl_context = None
    conn = aiohttp.TCPConnector(ssl_context=ssl_context)
    async with aiohttp.ClientSession(connector=conn) as session:
        wait = opts['wait']
        for n in range(opts['tries']):
            if n > 0:
                await asyncio.sleep(wait)
            try:
                start_time = time.time()
                response_data = None
                async with session.post(
                    url, json=data, timeout=opts['timeout']
                ) as resp:
                    if resp.status == 429:  # Too many requests.
                        wait = _get_rate_limit_wait(log, resp, opts)
                        continue
                    else:
                        wait = opts['wait']
                    resp.raise_for_status()
                    response_data = await resp.json()
                    return response_data
            except aiohttp.ClientError:
                log.exception('unhandled exception in http_post')
            except asyncio.TimeoutError:
                log.exception('timeout in http_post')
            finally:
                log.debug(
                    'http request',
                    request=data,
                    response=response_data,
                    duration=time.time() - start_time,
                )
        return {'status': 'error', 'error': c.ERR_SERVICE_UNAVAILABLE}
