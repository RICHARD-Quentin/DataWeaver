import json
import asyncio
import aiofiles
from yaml import CLoader as Loader, load
import time
from functions import crush, construct

config = {}

async def transform_field_value(data: dict, config: dict):
    return data

async def get_new_key(key: str):
    """
    Retrieves a new key from the given config based on the given key.

    Args:
        key (str): The original key.

    Returns:
        str: The new key.

    """
    simple_key = key.split('.')[-1]
    return config.get('mapping').get(key, config.get(simple_key))

async def map_fields(data: dict, final_result):
    """
    Maps the fields of the given data dictionary to new keys using the get_new_key function,
    and adds the mapped key-value pairs to the final_result dictionary.

    Args:
        data (dict): The input data dictionary.
        final_result (dict): The dictionary to store the mapped key-value pairs.

    Returns:
        None
    """
    for full_key, value in data.items():
        new_key = await get_new_key(full_key)
        if isinstance(new_key, list):
            for key in new_key:
                final_result[key] = value

        if isinstance(new_key, str):
            final_result[new_key] = value

async def parse_entry(object: dict, final_result, prefix: str = ''):
    """
    Parse the given object recursively and update the final_result dictionary with the extracted fields.

    Parameters:
        object (dict): The object to be parsed.
        final_result (dict): The dictionary to store the extracted fields.
        prefix (str, optional): The prefix to be added to the extracted field names. Defaults to ''.
    """
    for key, value in object.items():
        full_key = f'{prefix}.{key}' if prefix else key
        if isinstance(value, dict):
            await parse_entry(value, final_result, full_key)
        elif isinstance(value, list):
            for i, item in enumerate(value):
                await parse_entry(item, final_result, full_key)
        else:
            await map_fields({full_key: value}, final_result)

    for key, value in config.get('additionalFields').items():
        final_result[key] = value

async def process_entry(entry):
    final_result = {}
    flat_object = crush(entry)
    await parse_entry(flat_object, final_result)
    return construct(final_result)

async def process_entries(entries):
    """
    Process a list of entries asynchronously.

    Args:
        entries (list): A list of entries to process.

    Returns:
        list: A list of constructed objects.

    """
    final_list = []
    tasks = [process_entry(entry) for entry in entries]
    final_list = await asyncio.gather(*tasks)
    return final_list

async def load_config(config=config):
    """
    Loads the configuration from a JSON file.

    Returns:
        dict: The configuration dictionary.

    """
    print('Loading config...')

    async with aiofiles.open('./config/config.yml', 'r', encoding='utf8') as file:
        content = await file.read()

    config.update(load(content, Loader=Loader))
    
    if config.get('mapping') is None:
        raise Exception('Invalid config file!')
    if config.get('additionalFields') is None:
        config['additionalFields'] = {}
    
    print('Config loaded!')

async def save_result_to_file(result, file_path):
    # Asynchronously save the result to a file
    async with aiofiles.open(file_path, 'w') as file:
        await file.write(json.dumps(result))

async def weave_entry(data, config, *args, **kwargs):
    """
    Weaves the data with the given configuration.

    Args:
        data (dict): The input data.
        config (dict): The configuration.

    Returns:
        dict: The weaved data.
    """
    await load_config(config)
    result = await process_entry(data)

    if 'file_path' in kwargs and isinstance(kwargs['file_path'], str):
        await save_result_to_file(result, kwargs['file_path'])

    return result

async def weave_entries(data: list[dict], config: dict, *args, **kwargs):
    """
    Weaves the data with the given configuration.

    Args:
        data (dict): The input data.
        config (dict): The configuration.

    Returns:
        dict: The weaved data.
    """
    await load_config(config)
    result = await process_entries(data)

    if 'file_path' in kwargs and isinstance(kwargs['file_path'], str):
        await save_result_to_file(result, kwargs['file_path'])

    return result

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