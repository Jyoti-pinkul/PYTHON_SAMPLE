import pytest
import hashlib
from src.json_obj_mapping import match_json_obj_files, calculate_checksum  # Adjust based on your module path

@pytest.fixture
def mock_os_listdir(mocker):
    """Mock os.listdir to simulate directory contents."""
    return mocker.patch("os.listdir", return_value=[
        "a.json", "a.0.obj", "a.1.obj",
        "b.json", "b.0.obj",
        "c.json", "c.0.obj", "c.1.obj",
        "dataset.res"
    ])

@pytest.fixture
def mock_open_res_file(mocker):
    """Mock opening a .res file to simulate reading its content."""
    return mocker.patch("builtins.open", mocker.mock_open(read_data="test content"))

def test_match_json_obj_files(mock_os_listdir):
    """Test JSON to OBJ file matching logic without actual files."""
    result = match_json_obj_files("/fake/directory")  # Fake directory

    # Validate JSON to OBJ mappings
    assert "a.json" in result
    assert result["a.json"].obj_files == ["a.0.obj", "a.1.obj"]
    assert result["a.json"].obj_count() == 2

    assert "b.json" in result
    assert result["b.json"].obj_files == ["b.0.obj"]
    assert result["b.json"].obj_count() == 1

    assert "c.json" in result
    assert result["c.json"].obj_files == ["c.0.obj", "c.1.obj"]
    assert result["c.json"].obj_count() == 2

    # Validate .res file checksum storage
    assert "resource" in result
    assert "dataset.res" in result["resource"]

def test_calculate_checksum(mock_open_res_file):
    """Test checksum calculation without reading an actual file."""
    # Expected SHA-256 checksum for "test content"
    expected_checksum = hashlib.sha256(b"test content").hexdigest()

    assert calculate_checksum("/fake/dataset.res") == expected_checksum