# DataWeaver

A brief description of what this project does and who it's for. This project is an asynchronous data processing library designed to transform and process data entries efficiently, with a special focus on handling complex data structures.

## Features

- Asynchronous data processing for improved performance.
- Configuration-based processing for flexible data handling.
- Support for JSON and YAML configurations.
- File operations with `aiofiles` for non-blocking file I/O.
- Advanced mapping capabilities, allowing for complex key transformations involving nested objects and arrays.

## Advanced Mapping Capabilities

One of the standout features of this library is its ability to handle complex keys for data transformations. This allows for precise control over how nested data structures are transformed and outputted. Here's how it works:

- **Dot Notation for Nested Objects**: If you want to access data within nested objects, you can use a dot (`.`) in the key. For example, `parent.child` will access the `child` key within a `parent` object.
- **Digits for Array Indices**: When a nested key is a digit, the library interprets it as an array index. For example, `parent.0.child` accesses the `child` key of the first object in an array located at the `parent` key.
- **Automatically Creating Arrays**: If the transformation requires placing items into an array based on their keys, the library will automatically create and manage these arrays for you. This is particularly useful when dealing with dynamic data structures.

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install data-weaver.

```bash
pip install DataWeaver
```

## Usage

This document provides detailed documentation for two asynchronous functions used for processing data entries based on a given configuration.

## `weave_entry` Function

The `weave_entry` function asynchronously processes a single entry of data based on a given configuration, optionally saving the processed result to a file. This function is designed for handling individual data entries.

### Parameters

- `data` (dict): The input data to be processed. This should be a dictionary representing a single entry.
- `config` (dict): The configuration settings used for processing the data. This dictionary should contain all necessary parameters and settings required by the `load_config` and `process_entry` functions.
- `*args`: Variable length argument list. Allows for additional arguments to be passed, which might be required by future extensions or modifications without changing the function signature.
- `**kwargs`: Arbitrary keyword arguments. This function looks for a specific keyword argument:
  - `file_path` (str, optional): If provided and is a string, the function will save the processed data to the specified file path using `save_result_to_file`.
  If you don"t provide an extention to the file, by default it will register as json, supported extentions are json, csv, yml and yaml

### Returns

- `dict`: A dictionary containing the processed data based on the input and the configuration.

### Example Usage

```python
from data_weaver import weave_entry
result = await weave_entry(data, config, file_path="path/to/save/result.json")
```

---

## `weave_entries` Function

The `weave_entries` function asynchronously processes a list of data entries based on a given configuration, optionally saving the processed results to a file. This function is designed for handling multiple data entries in bulk.

### Parameters

- `data` (list[dict]): A list of dictionaries, where each dictionary represents an input data entry to be processed.
- `config` (dict): The configuration settings used for processing the data entries. This dictionary should contain all necessary parameters and settings required by the `load_config` and `process_entries` functions.
- `*args`: Variable length argument list. Allows for additional arguments to be passed, which might be required by future extensions or modifications without changing the function signature.
- `**kwargs`: Arbitrary keyword arguments. This function looks for a specific keyword argument:
  - `file_path` (str, optional): If provided and is a string, the function will save the processed data to the specified file path using `save_result_to_file`.
  If you don"t provide an extention to the file, by default it will register as json, supported extantion are json, csv, yml and yaml

### Returns

- `dict`: A dictionary containing the processed data for all entries based on the input list and the configuration.

### Example Usage

```python
from data_weaver import weave_entries
results = await weave_entries(data_list, config, file_path="path/to/save/results.json")
```

There is also two function that you can use to transform your data from utils:

## `crush` Function

The `crush` function flattens a nested dictionary or list into a flat dictionary with keys representing the paths to each value.

### Parameters

- `nested_dict` (dict | list): The nested dictionary or list to be flattened.
- `parent_key` (str, optional): The base path for keys in the flattened dictionary. Defaults to an empty string.
- `sep` (str, optional): The separator used between keys in the flattened dictionary. Defaults to a period (`.`).

### Returns

- `dict`: A flat dictionary where each key is a path composed of original keys concatenated by the specified separator, leading to the corresponding value in the nested structure.

### Example Usage

```python
from data_weaver import crush

nested = {'a': {'b': {'c': 1, 'd': 2}}, 'e': [3, 4, {'f': 5}]}
flat = crush(nested)
print(flat)
// {'a.b.c': 1, 'a.b.d': 2, 'e.0': 3, 'e.1': 4, 'e.2.f': 5}
```

---

## `construct` Function

The `construct` function reconstructs a nested dictionary or list from a flat dictionary, where each key represents a path to its corresponding value.

### Parameters

- `flat_dict` (dict): The flat dictionary to be reconstructed. Keys should be paths with parts separated by periods (`.`), representing the structure of the resulting nested dictionary or list.

### Returns

- The reconstructed nested dictionary or list based on the paths represented by the keys in the input flat dictionary.

### Example Usage

```python
from data_weaver import construct
flat = {'a.b.c': 1, 'a.b.d': 2, 'e.0': 3, 'e.1': 4, 'e.2.f': 5}
nested = construct(flat)
print(nested)
// {'a': {'b': {'c': 1, 'd': 2}}, 'e': [3, 4, {'f': 5}]}
```

## Configuration

Define mappings and additional fields required for processing your data in a Dict. Here's an example that demonstrates handling complex keys:

```python
    config = {
        'mapping': {
            'person.name': 'fullName',
            'person.details.age': 'age',
            'person.children.0.name': 'firstChildName'
        },
        'additionalFields': {
            'person.details.age': 'yearsOld'
        }
    }
```

### Exemple

With this config, the object below:

```json
{
  "person": {
    "name": "John Doe",
    "details": {
      "age": 30
    },
    "children": [
      {
        "name": "Alice"
      },
      {
        "name": "Bob"
      }
    ]
  }
}
```

Will be transformed to:

```json
{
  "fullName": "John Doe",
  "age": 30,
  "firstChildName": "Alice",
  "newField": "newValue"
}
```

### Other example
  
```python
    config = {
        'mapping': {
            'fullName': 'person.name',
            'age': 'person.details.age',
            'firstChildName': 'person.children.0.name'
        },
        'additionalFields': {
            'newField': 'newValue'
        }
    }
```

The object below:

```json
{
  "fullName": "John Doe",
  "age": 30,
  "firstChildName": "Alice",
}
```

Will be transformed to:

```json
{
  "person": {
    "name": "John Doe",
    "details": {
      "age": 30
    },
    "children": [
      {
        "name": "Alice"
      }
    ]
  },
  "newField": "newValue"
}
```

You can also map the same field to multiple keys:

```python
config = {
  'mapping': {
    'fullName': ['person.name', 'person.details.fullName']
  }
}
```

This object

```json
{
  "fullName": "John Doe"
}
```

Will be transformed to:

```json
{
  "person": {
    "name": "John Doe",
    "details": {
      "fullName": "John Doe"
    }
  }
}
```

This configuration extracts the name and age from a person object, the name of the first child in a children array, and adds a new field newField.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
