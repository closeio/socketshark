import asyncio
import ssl

import aiohttp


async def http_post(shark, url, data):
    opts = shark.config['HTTP']
    if 'ssl_cafile' in opts:
        ssl_context = ssl.create_default_context(cafile=opts['ssl_cafile'])
    else:
        ssl_context = None
    conn = aiohttp.TCPConnector(ssl_context=ssl_context)
    async with aiohttp.ClientSession(connector=conn) as session:
        for n in range(opts['tries']):
            if n > 0:
                await asyncio.sleep(opts['wait'])
            try:
                async with session.post(url, json=data,
                                        timeout=opts['timeout']) as resp:
                    print('RETURN', await resp.text())
                    return await resp.json()
            except aiohttp.ClientError:
                # TODO: log
                print('ERROR')
                import traceback
                traceback.print_exc()
        return {'status': 'error', 'error': 'Service unavailable.'}
