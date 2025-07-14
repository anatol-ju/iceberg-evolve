import pytest
from iceberg_evolve.schema import Schema

def test_schema_normalization():
    """Test that field names are normalized to lowercase and type definitions are preserved."""
    schema_dict = {
        "properties": {
            "Email": {"type": "string"},
            "id": {"type": "int"}
        }
    }
    schema = Schema(schema=schema_dict)
    assert schema.fields == {
        "email": {"type": "string"},
        "id": {"type": "int"}
    }

def test_missing_name_raises():
    """Test that a field without a type raises a ValueError."""
    schema_dict = {
        "properties": {
            "some_field": {}
        }
    }
    with pytest.raises(ValueError):
        Schema(schema=schema_dict)

def test_missing_type_raises():
    """Test that a field with no type raises a ValueError."""
    schema_dict = {
        "properties": {
            "foo": {}
        }
    }
    with pytest.raises(ValueError):
        Schema(schema=schema_dict)

def test_to_dict():
    """Test that the schema can be converted back to its dictionary representation."""
    schema_dict = {
        "properties": {
            "id": {"type": "int"}
        }
    }
    schema = Schema(schema=schema_dict)
    assert schema.schema == schema_dict


# Additional tests for Schema coverage
def test_repr_returns_string():
    """Test that the __repr__ method returns a string representation of the schema."""
    schema_dict = {"properties": {"id": {"type": "int"}}}
    schema = Schema(schema=schema_dict)
    assert isinstance(repr(schema), str)


def test_from_file_reads_schema(tmp_path):
    """Test loading a schema from a local JSON file."""
    path = tmp_path / "schema.json"
    path.write_text('{"properties": {"id": {"type": "int"}}}')
    schema = Schema.from_file(str(path))
    assert schema.fields == {"id": {"type": "int"}}


def test_from_s3_reads_schema(monkeypatch):
    """Test loading a schema from an S3 bucket using a mocked boto3 client."""
    class MockS3Object:
        def get(self):
            return {"Body": MockBody()}

    class MockBody:
        def read(self):
            return b'{"properties": {"id": {"type": "int"}}}'

    class MockS3Resource:
        def Object(self, bucket, key):
            return MockS3Object()

    # Patch boto3.resource to return our mock
    import sys
    mock_boto3 = type("boto3", (), {"resource": lambda x: MockS3Resource()})
    monkeypatch.setitem(sys.modules, "boto3", mock_boto3)
    schema = Schema.from_s3("bucket", "key")
    assert schema.fields == {"id": {"type": "int"}}


def test_from_iceberg_uses_loader(monkeypatch):
    """Test loading a schema from an Iceberg table using a mocked catalog loader."""
    def mock_loader(table_name, catalog, config):
        return {"properties": {"id": {"type": "int"}}}

    import sys
    mock_catalog = type("catalog", (), {"load_table_schema": mock_loader})
    monkeypatch.setitem(sys.modules, "iceberg_evolve.catalog", mock_catalog)
    schema = Schema.from_iceberg("table", "glue")
    assert schema.fields == {"id": {"type": "int"}}
