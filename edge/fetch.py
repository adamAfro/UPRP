# scrap/fetch.py
async def parallel(data: list, callback:callable, delay=.1, timeout=10., limit=5, desc=''):

    import aiohttp, asyncio
    from tqdm.asyncio import tqdm

    timeout = aiohttp.ClientTimeout(total=timeout)
    limit = asyncio.Semaphore(limit)

    async def task_wrapper(x):
        async with limit:
            try: return await callback(session, x)
            except aiohttp.ClientResponseError: return None
            except asyncio.TimeoutError: return None
            finally: await asyncio.sleep(delay)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = [task_wrapper(x) for x in data]
        results = []
        for task in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc=desc):
            result = await task
            results.append(result)

    return results