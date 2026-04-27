import asyncio
import ssl
import time
from typing import TYPE_CHECKING
from urllib.parse import urlsplit, urlunsplit

import aiohttp
import structlog

from . import constants as c
from .types import HttpOptions, ServiceRequestData, ServiceResponse

if TYPE_CHECKING:
    from . import SocketShark


def _get_rate_limit_wait(
    log: structlog.stdlib.BoundLogger,
    resp: aiohttp.ClientResponse,
    opts: HttpOptions,
) -> float:
    """
    Return the number of seconds we should wait if rate limited.

    Args:
        log: logger to log to
        resp: a 429 HTTP response
        opts: HTTP options
    """
    max_wait = 3600

    wait: float = opts['wait']

    header_names = opts['rate_limit_reset_header_names'] or [
        opts['rate_limit_reset_header_name']
    ]
    header_value = None
    for header_name in header_names:
        if header_name in resp.headers:
            header_value = resp.headers[header_name]
            break
    if header_value:
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
    else:
        log.warn(
            'got a 429 but no rate limit reset header found in response',
        )
    return wait


def _scrub_url(url: str) -> str:
    """Scrub URL username and password."""
    url_parts = urlsplit(url)
    if url_parts.password is None:
        return url
    else:
        # url_parts tuple doesn't include password in _fields
        # so can't easily use _replace to get rid of password
        # and then call urlunsplit to reconstruct url.
        _, _, hostinfo = url_parts.netloc.rpartition('@')
        scrubbed_netloc = f'*****:*****@{hostinfo}'  # noqa: E231
        scrubbed_url_parts = url_parts._replace(netloc=scrubbed_netloc)
        return urlunsplit(scrubbed_url_parts)


async def http_post(
    shark: 'SocketShark', url: str, data: ServiceRequestData
) -> ServiceResponse:
    log = shark.log.bind(url=_scrub_url(url))
    opts: HttpOptions = shark.config['HTTP']
    ssl_context = (
        ssl.create_default_context(cafile=opts['ssl_cafile'])
        if opts.get('ssl_cafile')
        else None
    )
    conn = aiohttp.TCPConnector(ssl_context=ssl_context)
    async with aiohttp.ClientSession(connector=conn) as session:
        wait: float = opts['wait']
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
                    return ServiceResponse(response_data)
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
        return ServiceResponse(
            {'status': 'error', 'error': c.ERR_SERVICE_UNAVAILABLE}
        )
