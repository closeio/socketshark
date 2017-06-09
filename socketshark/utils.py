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
        for n in range(opts['tries']):
            if n > 0:
                await asyncio.sleep(opts['wait'])
            try:
                shark.log.debug('http request', url=url, data=data)
                async with session.post(url, json=data,
                                        timeout=opts['timeout']) as resp:
                    data = await resp.json()
                    shark.log.debug('http response', data=data)
                    return data
            except aiohttp.ClientError:
                shark.log.exception('unhandled exception in http_post')
        return {'status': 'error', 'error': c.ERR_SERVICE_UNAVAILABLE}
