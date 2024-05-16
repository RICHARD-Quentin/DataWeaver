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

Define mappings and additional fields required for processing your data in a Dict.

There are three main sections in the configuration file:

- `mapping`: Specifies how keys in the input data should be mapped to keys in the output data. The logical here is the following: `target_key: source_key`.
- `additionalFields`: Specifies additional fields that should be added to the output data. The logical here is the following: `target_key: value`.
- `transforms`: Specifies how different fields in the data are transformed using various functions. Each key represents a field or type of fields, and the associated value describes the transformation to be applied to that field.
- `default`: Specifies default values for fields that may not exist in the input data. This is useful for ensuring that the output data contains all expected fields, even if they are not present in the input data.

In the `default` section there is some sub sections.

- `static`: Specifies default values that are static and do not depend on the input data.
- `dynamic`: Specifies default values that are dynamic and depend on the input data.
- `transforms`: Specifies transformations that should be applied to default values.

 Here's an example that demonstrates handling complex keys:

### Configuration File: Mapping

This section of the configuration file specifies how keys in the input data should be mapped to keys in the output data. Each key represents a target key in the output data, and the associated value represents the source key in the input data.

The target key can be a simple key or a complex key with nested objects and arrays. The source key can also be a simple key or a complex key with nested objects and arrays. A dot (`.`) is used to represent nested objects, and a digit is used to represent array indices.

```python
    config = {
        'mapping': {
            'person.name': 'fullName',
            'person.details.age': 'age',
            'person.children.0.name': 'firstChildName'
        },
    }
```

### Exemple

With this config, the object below:

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
    "children": [
      {
        "name": "Alice"
      },
    ]
  }
}
```

You can also map the same field to multiple keys:

```python
config = {
  'mapping': {
    'person.details.fullName': 'fullName'
    'person.name': 'fullName'
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

And you can also map multiple fields to the same key:

```python
config = {
  'mapping': {
    'person.name': 'name',
    'person.lastName': 'lastName',
    'person.fullName': ['name', 'lastName'],
  }
}
```

This object

```json
{
  "name": "John",
  "lastName": "Doe"
}
```

Will be transformed to:

```json
{
  "person": {
    "name": "John",
    "lastName": "Doe",
    "fullName": ["John", "Doe"]
  }
}
```

### Configuration File: Additional Fields

This section of the configuration file specifies additional fields that should be added to the output data. Each key represents a target key in the output data, and the associated value represents the value to be assigned to that key. It follow the same logic as the mapping section with dot an numbers notation. But now the value that is passed to the key is not a field but the value you want to assign.

```json
{
  "mapping": {
    "person.name": "fullName",
    "person.details.age": "age",
  },
  "additionalFields": {
      "type": "employee",
  }
}
```

The object below:

```json
{
  "fullName": "John Doe",
  "age": 30,
}
```

Will be transformed to:

```json
{
  "person": {
    "name": "John Doe",
    "details": {
      "age": 30
    }
  },
  "type": "employee"
}
```

### Configuration File: Transforms

This section of the configuration file specifies how different fields in the data are transformed using various functions. Each key represents a field or type of fields, and the associated value describes the transformation to be applied to that field. These transformations can include formatting, concatenation, type conversion, and more.

| Function Name | Description |
| --- | --- |
| `capitalize` | Converts the first character of the string to uppercase and the rest to lowercase. |
| `lower` | Converts all characters in the string to lowercase. |
| `upper` | Converts all characters in the string to uppercase. |
| `title` | Converts the first character of each word to uppercase and the remaining characters of each word to lowercase. |
| `concat(delimiter=' ')` | Concatenates list elements into a single string with elements separated by the specified delimiter. Default is a space. /!\ All elements must be strings |
| `join(delimiter=' ')` | Joins elements of a list into a single string with elements separated by the specified delimiter. Default is a space. |
| `prefix(string='prefix-')` | Prepends the specified string to the beginning of the target string. Default prefix is "prefix-". |
| `suffix(string='-suffix')` | Appends the specified string to the end of the target string. Default suffix is "-suffix". |
| `split(delimiter=' ')` | Splits the string into a list of substrings around the specified delimiter. Default is a space. |
| `replace(old, new)` | Replaces occurrences of a substring (old) within the string with another substring (new). Options must specify old and new. |
| `regex(pattern, replace)` | Applies a regular expression pattern to the string and replaces matches with the specified replacement string. Options must specify both pattern and replace. |
| `parse_type(typename)` | Converts the string to the specified type (typename). Valid types include str, bool, int, and float. |

## Function Descriptions

### 1. Text Case Functions

- `capitalize`: Converts the first character of the string to uppercase and the rest to lowercase.
  Example:

  ```json
  {
    "mapping": {
      "person.name": "fullName",
    },
    "transforms": {
      "person.name": "capitalize",
    }
  }
  ```

  The object below:

  ```json
  {
    "fullName": "john doe",
  }
  ```

  Will be transformed to:

  ```json
  {
    "fullName": "John doe",
  }
  ```

- `lower`: Converts all characters in the string to lowercase.
  Example:

  ```json
  {
    "mapping": {
      "person.name": "fullName",
    },
    "transforms": {
      "person.name": "lower",
    }
  }
  ```

  The object below:

  ```json
  {
    "fullName": "JOHN DOE",
  }
  ```

  Will be transformed to:

  ```json
  {
    "fullName": "john doe",
  }
  ```

- `upper`: Converts all characters in the string to uppercase.
  Example:

  ```json
  {
    "mapping": {
      "person.name": "fullName",
    },
    "transforms": {
      "person.name": "upper",
    }
  }
  ```

  The object below:

  ```json
  {
    "fullName": "john doe",
  }
  ```

  Will be transformed to:

  ```json
  {
    "fullName": "JOHN DOE",
  }
  ```

- `title`: Converts the first character of each word to uppercase and the remaining characters of each word to lowercase.
  Example:

  ```json
  {
    "mapping": {
      "person.name": "fullName",
    },
    "transforms": {
      "person.name": "title",
    }
  }
  ```

  The object below:

  ```json
  {
    "fullName": "john doe",
  }
  ```

  Will be transformed to:

  ```json
  {
    "fullName": "John Doe",
  }
  ```

### 2. String Manipulation Functions

- `concat(delimiter=' ')`: Concatenates list elements into a single string with elements separated by the specified delimiter. Default is a space.

    ```json
  {
    "mapping": {
      "person.fullName": ["firstName", "lastName"],
    },
    "transforms": {
      "person.fullName": "concat(delimiter=' ')",
    }
  }
  ```

  The object below:

  ```json
  {
    "firstName": "John",
    "lastName": "Doe",
  }
  ```

  Will be transformed to:

  ```json
  {
    "person": {
      "fullName": "John Doe",
    }
  }
  ```

- `prefix(string='prefix-')`: Prepends the specified string to the beginning of the target string. Default prefix is "prefix-".
  Example:

  ```json
  {
    "mapping": {
      "person.name": "fullName",
    },
    "transforms": {
      "person.name": "prefix(string='hello-')",
    }
  }
  ```

  The object below:

  ```json
  {
    "fullName": "world",
  }
  ```

  Will be transformed to:

  ```json
  {
    "person": {
      "name": "hello-world",
    }
  }
  ```

- `suffix(string='-suffix')`: Appends the specified string to the end of the target string. Default suffix is "-suffix".
  Example:

  ```json
  {
    "mapping": {
      "person.name": "fullName",
    },
    "transforms": {
      "person.name": "suffix(string='-world')",
    }
  }
  ```

  The object below:

  ```json
  {
    "fullName": "hello",
  }
  ```

  Will be transformed to:

  ```json
  {
    "person": {
      "name": "hello-world",
    }
  }
  ```

- `split(delimiter=' ')`: Splits the string into a list of substrings around the specified delimiter. Default is a space.
  Example:

  ```json
  {
    "mapping": {
      "person.fullName": "fullName",
    },
    "transforms": {
      "person.fullName": "split(delimiter=' ')",
    }
  }
  ```

  The object below:

  ```json
  {
    "fullName": "John Doe",
  }
  ```

  Will be transformed to:

  ```json
  {
    "person": {
      "fullName": ["John", "Doe"],
    }
  }
  ```

- `join(delimiter=' ')`: Joins elements of a list into a single string with elements separated by the specified delimiter. Default is a space.

    ```json
  {
    "mapping": {
      "person.fullName": ["firstName", "lastName"],
    },
    "transforms": {
      "person.fullName": "join(delimiter=' ')",
    }
  }
  ```

  The object below:

  ```json
  {
    "firstName": "John",
    "lastName": "Doe",
  }
  ```

  Will be transformed to:

  ```json
  {
    "person": {
      "fullName": "John Doe",
    }
  }
  ```

### 3. Replacement and Pattern Matching Functions

- `replace(old, new)`: Replaces occurrences of a substring (old) within the string with another substring (new). Options must specify old and new.
  Example:

  ```json
  {
    "mapping": {
      "person.name": "fullName",
    },
    "transforms": {
      "person.name": "replace(old='world', new='hello')",
    }
  }
  ```

  The object below:

  ```json
  {
    "fullName": "world",
  }
  ```

  Will be transformed to:

  ```json
  {
    "person": {
      "name": "hello",
    }
  }
  ```

- `regex(pattern, replace)`: Applies a regular expression pattern to the string and replaces matches with the specified replacement string. Options must specify both pattern and replace.
  Example:

  ```json
  {
    "mapping": {
      "person.name": "fullName",
    },
    "transforms": {
      "person.name": "regex(pattern='[a-z]+', replace='X')",
    }
  }
  ```

  The object below:

  ```json
  {
    "fullName": "world",
  }
  ```

  Will be transformed to:

  ```json
  {
    "person": {
      "name": "X",
    }
  }
  ```

### 4. Type Parsing Functions

- `parse_type(typename)`: Converts the string to the specified type (typename). Valid types include str, bool, int, and float.
  Example:

  ```json
  {
    "mapping": {
      "age_mapped": "age",
      "is_student_mapped": "is_student", 
      "is_teacher_mapped": "is_teacher", 
      "salary_mapped": "salary",
      "student_id_mapped": "student_id",
    },
    "transforms": {
      "age_mapped": "parse_type(typename='int')",
      "is_student_mapped": "parse_type(typename='bool')",
      "is_teacher_mapped": "parse_type(typename='bool')",
      "salary_mapped": "parse_type(typename='float')",
      "student_id_mapped": "parse_type(typename='int')",
    }
  }
  ```

  The object below:

  ```json
  {
    "age": "30",
    "is_student": "True",
    "is_teacher": "False",
    "salary": "3000.50",
    "student_id": 12345,
  }
  ```

  Will be transformed to:

  ```json
  {
    "age_mapped": 30,
    "is_student_mapped": true,
    "is_teacher_mapped": false,
    "salary_mapped": 3000.50,
    "student_id_mapped": "12345",
  }
  ```

PS : bool type is case insensitive, so you can pass :

- "true", "True", "TRUE", "yes",  "Yes", "YES", "y", "Y" it will be converted to True
- "no", "No", "NO", "n", "N", "false", "False", "FALSE"  it will be converted to False

### Chaining Transformations

You can chain multiple transformations together by describe them in a list. This allows you to apply multiple transformations to a single field in a specific order.

```json
{
  "mapping": {
    "person.cn": ["firstName", "lastName"],
  },
  "transforms": {
    "person.name": ["lower", "concat(delimiter='.')"],
  }
}
```

The object below:

```json
{
  "firstName": "John",
  "lastName": "Doe",
}
```

Will be transformed to:

```json
{
  "person": {
    "name": "john.doe",
  }
}
```

### Default Values

You can also specify default values for fields that may not exist in the input data. This is useful for ensuring that the output data contains all expected fields, even if they are not present in the input data.

```json
{
  "mapping": {
    "person.name": "fullName",
    "person.age": "age",
  },
  "default": {
    "static": {
      "person.age": 0,
    },
    "dynamic": {
      "person.name": ["firstName", "lastName"],
    }
  }
}

```

The object below:

```json
{
  "fullName": "John Doe",
  "firstName": "John",
  "lastName": "Doe",
  "age": 30,
}

```

Will be transformed to:

```json
{
  "person": {
    "name": "John Doe",
    "age": 30,
  }
}

```

But if there is no age in the input data, and no fullname the output will be:

```json
{
  "person": {
    "name": ["John", "Doe"],
    "age": 0,
  }
}

```

You can also add transformations to default values:

```json
{
  "mapping": {
    "person.name": "fullName",
    "person.age": "age",
  },
  "default": {
    "static": {
      "person.age": 0,
    },
    "dynamic": {
      "person.name": ["firstName", "lastName"],
    },
    "transforms": {
      "person.name": ["lower", "concat(delimiter='.')"],
    }
  }
}

```

The object below:

```json
{
  "fullName": "John Doe",
  "firstName": "John",
  "lastName": "Doe",
}

```

Will be transformed to:

```json
{
  "person": {
    "name": "john.doe",
    "age": 0,
  }
}

```

### Advanced Configuration

Here's an example of a more complex configuration file that demonstrates the use of multiple mapping, additional fields, and transformation functions:

```json
config = {
  "mapping": {
    "uid": "login",
    "uid2": "login2",
    "fullname": ["nom", "prenom"],
  },
  "transforms": {
    "fullname": ["join(delimiter=' ')"],
    "uid": ["replace(old='.', new='')"],
  },
  "default": {
    "dynamic": {
      "uid": ["nom", "prenom"]
    },
    "static": {
      "uid2": "test"
    },
    "transforms": {
      "uid": ["join(delimiter='.')","lower", "regex(pattern='(?<=\\b\\w)\\w+(?=\\.)', replace='')"],
    },
  }
}
```

This configuration file specifies the following:

- The `uid` field in the output data should be mapped to the `login` field in the input data.
- The `uid2` field in the output data should be mapped to the `login2` field in the input data.
- The `fullname` field in the output data should be mapped to the `nom` and `prenom` fields in the input data. The transformations applied to this field are:
  - Convert the field to lowercase.
  - Join the elements of the list with a period (`.`) delimiter.
  - Apply a regular expression to remove all characters after the first period (`.`) in the string.
- The `uid` field should be created by joining the `nom` and `prenom` fields in the input data, converting the result to lowercase, and removing all characters after the first period (`.`) in the string.
- The `uid2` field should have a default value of `test`.
- The `uid` field should have a default value of `nom` and `prenom` in the input data, converted to lowercase, and with all characters after the first period (`.`) removed.

The following input data:

```json
{
  "login": "John.Doe",
  "login2": "Jane.Doe",
  "nom": "John",
  "prenom": "Doe"
}
```

Will be transformed to:

```json
{
    "uid": "JohnDoe",
    "uid2": "Jane.Doe",
    "fullname": "John Doe"
}
```

And this one :

```json
{
  "login2": "Jane.Doe",
  "nom": "John",
  "prenom": "Doe"
}
```

Will be transformed to:

```json
{
    "uid": "j.doe",
    "uid2": "Jane.Doe",
    "fullname": "John Doe"
}
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.
