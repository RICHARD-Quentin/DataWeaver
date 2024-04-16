
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
    
def concat(values: list, separator=' ') -> str:
    if all(isinstance(value, str) for value in values):
        return separator.join(values)
    else:
        raise TypeError("All values in concat must be strings")
    
def parseType(value, typename: str) -> type:
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

def join(values: list, separator: str = ' ') -> str:
    return separator.join(values)

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

def regex(value: str | list | dict, pattern: str, repl: str) -> str:
    def regex_replace(val):
        return re.sub(pattern, repl, val)
    return apply_to_value(value, regex_replace)

TRANSFORMATIONS: Dict[str, Callable[..., Any]] = {
    "capitalize": capitalize,
    "lower": lower,
    "title": title,
    "upper": upper,
    "concat": lambda args: concat(args[0], args[1]) if len(args) > 1 else concat(args[0]),
    "parseType": lambda args: parseType(args[0], args[1]),
    "prefix": lambda args: prefix(args[0], args[1]),
    "suffix": lambda args: suffix(args[0], args[1]),
    "split": lambda args: split(args[0], args[1]) if len(args) > 1 else split(args[0]),
    "join": lambda args: join(args[0], args[1]) if len(args) > 1 else join(args[0]),
    "replace": lambda args: replace(args[0], args[1], args[2]),
    "regex": lambda args: regex(args[0], args[1], args[2])
}

def parseTransform(transform: str, value: Any) -> Any:
    func_name, *args = transform.replace(")", "").split("(")
    func = TRANSFORMATIONS.get(func_name)
    if not func:
        raise ValueError(f"No such transformation {func_name}")
    args = [arg.strip() for arg in args[0].split(",")] if args else []
    return func([value] + args)