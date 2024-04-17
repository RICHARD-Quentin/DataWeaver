
import re
from typing import Any, Callable, Dict

def apply_to_value(value, func, *args, **kwargs):
    if isinstance(value, dict):
        return {key: apply_to_value(val, func, *args, **kwargs) for key, val in value.items()}
    elif isinstance(value, list):
        return [apply_to_value(val, func, *args, **kwargs) for val in value]
    else:
        return func(value, *args, **kwargs)

def capitalize(value: str) -> str:
    def capitalize_val(val):
        return val.capitalize()
    return apply_to_value(value, capitalize_val)
    
def concat(values: list, delimiter=' ') -> str:
    if all(isinstance(value, str) for value in values):
        return delimiter.join(values)
    else:
        raise TypeError("All values in concat must be strings")
    
def parse_type(value, typename: str) -> type:
    try:
        # Mapping string to actual type
        type_map = {
            "int": int,
            "float": float,
            "str": str,
            "bool": lambda x: x.lower() in ['true', '1', 't', 'yes', 'y']
        }
        def parse(val):
            return type_map[typename](val)
        return apply_to_value(value, parse)
    except KeyError:
        raise ValueError(f"Invalid type {typename}")
    
def prefix(value: str | list | dict, prefix: str) -> str:
    def prefix_val(val):
        return f"{prefix}{val}"
    return apply_to_value(value, prefix_val)

def suffix(value: str | list | dict, suffix: str) -> str:
    def suffix_val(val):
        return f"{val}{suffix}"
    return apply_to_value(value, suffix_val)

def split(value: str, delimiter: str = ' ') -> list:
    return value.split(delimiter)

def join(values: list, delimiter: str = ' ') -> str:
    return delimiter.join(values)

def lower(value: str | list | dict) -> str:
    def lower_val(val):
        return val.lower()
    return apply_to_value(value, lower_val)

def title(value: str | list | dict) -> str:
    def title_val(val):
        return val.title()
    return apply_to_value(value, title_val)

def upper(value: str | list | dict) -> str:
    def upper_val(val):
        return val.upper()
    return apply_to_value(value, upper_val)

def replace(value: str | list | dict, old: str, new: str) -> str:
    def replace_val(val):
        return val.replace(old, new)
    return apply_to_value(value, replace_val)

def regex(value: str | list | dict, pattern: str, replace: str) -> str:
    def regex_replace(val):
        return re.sub(pattern, replace, val)
    return apply_to_value(value, regex_replace)

TRANSFORMATIONS: Dict[str, Callable[..., Any]] = {
    "capitalize": capitalize,
    "lower": lower,
    "title": title,
    "upper": upper,
    "concat": lambda value, delimiter='': concat(value, delimiter),
    "parse_type": lambda value, typename: parse_type(value, typename),
    "prefix": lambda value, string: prefix(value, string),
    "suffix": lambda value, string: suffix(value, string),
    "split": lambda value, delimiter=None: split(value, delimiter),
    "join": lambda value, delimiter='': join(value, delimiter),
    "replace": lambda value, old, new: replace(value, old, new),
    "regex": lambda value, pattern, replace: regex(value, pattern, replace)
}

def parse_args(args: str) -> list:
    kwargs = {}
    for arg in args:
        key_values = arg.split(", ")
        for key_value in key_values:
            key, value = key_value.split('=')
            kwargs[key.strip()] = eval(value)  # Using eval to convert the string 'X' to X without quotes
    return kwargs

async def parse_transform(transform: str, value: Any) -> Any:
    print('parseTransform', transform, value)
    func_name, *args = transform.replace(")", "").split("(")
    print('parseTransform', func_name, args)
    func = TRANSFORMATIONS.get(func_name)
    if not func:
        print(f"Invalid transform function: {func_name} for value: {value}")
        return value
    try :
        kwargs = parse_args(args)
        return func(value, **kwargs)
    except Exception as e:
        print(e)
        print(f"Error in transform function: {func_name} for value: {value} with args: {args}")
        return value