import os
import fnmatch
import hashlib
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Union

@dataclass
class JsonObjMapping:
    """Dataclass to store JSON file, corresponding OBJ files, and their count."""
    json_file: str
    obj_files: List[str] = field(default_factory=list)

    def obj_count(self) -> int:
        """Returns the number of associated OBJ files."""
        return len(self.obj_files)

def calculate_checksum(file_path: str) -> str:
    """
    Calculates the SHA-256 checksum of a given file.

    :param file_path: Path to the .res file.
    :return: Hexadecimal SHA-256 checksum string.
    """
    hash_sha256 = hashlib.sha256()
    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_sha256.update(chunk)
        return hash_sha256.hexdigest()
    except FileNotFoundError:
        return "File not found"

def add_res_file_checksum(directory: str, json_obj_map: Dict[str, Union[JsonObjMapping, Dict[str, str]]]) -> None:
    """
    Finds the .res file in the directory, calculates its checksum, and adds it to the JSON object dictionary.

    :param directory: Path to the directory.
    :param json_obj_map: Dictionary to store the .res file checksum under "resource".
    """
    files = [file for file in os.listdir(directory) if file.endswith(".res")]

    if len(files) == 1:
        res_file = files[0]
        res_path = os.path.join(directory, res_file)
        checksum = calculate_checksum(res_path)

        # Add .res file checksum under a "resource" key in json_obj_map
        json_obj_map["resource"] = {res_file: checksum}
    elif len(files) > 1:
        print("Warning: More than one .res file found. Skipping checksum calculation.")

# Existing function (unchanged)
def match_json_obj_files(directory: str) -> Dict[str, Union[JsonObjMapping, Dict[str, str]]]:
    """
    Identifies JSON files and matches corresponding .obj files.

    :param directory: Path to the directory containing JSON and OBJ files.
    :return: Dictionary where JSON filenames are keys, and JsonObjMapping objects as values.
    """
    files = os.listdir(directory)

    # Identify all JSON files
    json_files = [file for file in files if file.endswith(".json")]

    # Create a mapping dictionary
    json_obj_map: Dict[str, Union[JsonObjMapping, Dict[str, str]]] = {}

    for json_file in json_files:
        base_name = os.path.splitext(json_file)[0]  # Extract base name without extension

        # Find matching .obj files
        matching_objs = sorted(
            [file for file in files if fnmatch.fnmatch(file, f"{base_name}.*.obj")]
        )

        # Store in dataclass and add to dictionary
        json_obj_map[json_file] = JsonObjMapping(json_file=json_file, obj_files=matching_objs)

    # Add .res file checksum to the dictionary
    add_res_file_checksum(directory, json_obj_map)

    return json_obj_map

# Example usage:
directory = "/path/to/your/directory"
result = match_json_obj_files(directory)

# Print JSON-OBJ mapping and .res checksum
for key, value in result.items():
    if isinstance(value, JsonObjMapping):
        print(f"{value.json_file}: {value.obj_files} (Count: {value.obj_count()})")
    elif key == "resource":
        res_file, checksum = list(value.items())[0]
        print(f"Resource -> {res_file}: Checksum -> {checksum}")




"""
a.json: ['a.0.obj', 'a.1.obj', 'a.2.obj'] (Count: 3)
b.json: ['b.0.obj'] (Count: 1)
c.json: ['c.0.obj', 'c.1.obj'] (Count: 2)
x.json: [] (Count: 0)

Resource -> dataset.res: Checksum -> 5f3665a1c1e23e9fdfb8300adbbce472c1b8e3d1bbf1a2...

""

"""
output: json :

{
    "a.json": {
        "json_file": "a.json",
        "obj_files": ["a.0.obj", "a.1.obj", "a.2.obj"]
    },
    "b.json": {
        "json_file": "b.json",
        "obj_files": ["b.0.obj"]
    },
    "c.json": {
        "json_file": "c.json",
        "obj_files": ["c.0.obj", "c.1.obj"]
    },
    "x.json": {
        "json_file": "x.json",
        "obj_files": []
    },
    "resource": {
        "dataset.res": "5f3665a1c1e23e9fdfb8300adbbce472c1b8e3d1bbf1a2..."
    }
}

"""