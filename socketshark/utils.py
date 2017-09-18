import asyncio
import ssl

import aiohttp


from . import constants as c


async def http_post(shark, url, data):
    opts = shark.config['HTTP']
    if opts.get('ssl_cafile'):
        ssl_context = ssl.create_default_context(cafile=opts['ssl_cafile'])
    else:
        ssl_context = None
    conn = aiohttp.TCPConnector(ssl_context=ssl_context)
    async with aiohttp.ClientSession(connector=conn) as session:
        wait = opts['wait']
        reset_header = opts['rate_limit_reset_header_name']
        for n in range(opts['tries']):
            if n > 0:
                await asyncio.sleep(wait)
            try:
                shark.log.debug('http request', url=url, data=data)
                async with session.post(url, json=data,
                                        timeout=opts['timeout']) as resp:
                    if resp.status == 429:  # Too many requests.
                        if reset_header and reset_header in resp.headers:
                            try:
                                wait = int(resp.headers[reset_header])
                                if wait < 0:
                                    wait = opts['wait']
                            except ValueError:
                                wait = opts['wait']
                        continue
                    resp.raise_for_status()
                    data = await resp.json()
                    shark.log.debug('http response', data=data)
                    return data
            except aiohttp.ClientError:
                shark.log.exception('unhandled exception in http_post')
        return {'status': 'error', 'error': c.ERR_SERVICE_UNAVAILABLE}
