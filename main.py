import asyncio
from datetime import time
import json

import aiofiles

from data_weaver.main import load_config, process_entries, save_result_to_file


async def main():
    """
    This function is the entry point of the program.
    It reads the content of a JSON file, processes the entries,
    and writes the final list to another JSON file.
    """
    print('Starting...')
    start = time.time()
    await load_config()

    async with aiofiles.open('./data/test.json', 'r', encoding='utf8') as file:
        content = await file.read()
        json_entries = json.loads(content)
    
    final_list = await process_entries(json_entries.get('data'))

    await save_result_to_file(final_list, './data/result.json')

    print('Done!')
    print(f'Time: {time.time() - start}')

if __name__ == "__main__":
    asyncio.run(main())