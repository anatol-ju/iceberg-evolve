import pytest

from iceberg_evolve.schema import Schema
from iceberg_evolve.utils import IcebergSchemaSerializer


def test_repr_returns_string():
    """Test that the __repr__ method returns a string representation of the schema."""
    schema_dict = {
        "schema-id": 0,
        "fields": [{"id": 1, "name": "id", "type": "int", "required": True}]
    }
    iceberg_schema = IcebergSchemaSerializer.from_dict(schema_dict)
    schema = Schema(iceberg_schema)
    assert isinstance(repr(schema), str)


def test_from_file_reads_schema(tmp_path):
    """Test loading a schema from a local JSON file."""
    path = tmp_path / "schema.json"
    path.write_text('{"schema-id": 0, "fields": [{"id": 1, "name": "id", "type": "int", "required": true}]}')
    schema = Schema.from_file(str(path))
    assert [f.name for f in schema.fields] == ["id"]


def test_from_file_invalid_extension():
    """Test that loading a schema from a file with an invalid extension raises a ValueError."""
    with pytest.raises(ValueError, match="Currently, only JSON files are supported for schema loading."):
        Schema.from_file("schema.txt")


def test_from_s3_reads_schema(monkeypatch):
    """Test loading a schema from an S3 bucket using a mocked boto3 client."""
    class MockS3Object:
        def get(self):
            return {"Body": MockBody()}

    class MockBody:
        def read(self):
            return b'{"schema-id": 0, "fields": [{"id": 1, "name": "id", "type": "int", "required": true}]}'

    class MockS3Resource:
        def Object(self, bucket, key):
            return MockS3Object()

    import sys
    mock_boto3 = type("boto3", (), {"resource": lambda x: MockS3Resource()})
    monkeypatch.setitem(sys.modules, "boto3", mock_boto3)

    schema = Schema.from_s3("bucket", "schema.json")
    assert [f.name for f in schema.fields] == ["id"]


def test_from_s3_invalid_extension():
    """Test that loading a schema from S3 with an invalid file extension raises a ValueError."""
    with pytest.raises(ValueError, match="Currently, only JSON files are supported for schema loading from S3."):
        Schema.from_s3("bucket", "schema.txt")


def test_from_iceberg_uses_loader(monkeypatch):
    """Test loading a schema from an Iceberg table using a mocked catalog loader."""
    from pyiceberg.schema import Schema as IcebergSchema
    from pyiceberg.types import NestedField, StringType

    mock_schema = IcebergSchema(
        NestedField(field_id=1, name="id", field_type=StringType(), required=True)
    )

    class MockCatalog:
        def load_table(self, table_name):
            class MockTable:
                schema = mock_schema
            return MockTable()

    import iceberg_evolve.schema
    monkeypatch.setattr(iceberg_evolve.schema, "load_catalog", lambda name, **kwargs: MockCatalog())

    schema = Schema.from_iceberg("table", "glue")
    assert [f.name for f in schema.fields] == ["id"]

def test_schema_property_returns_iceberg_schema():
    """Test that the 'schema' property returns the underlying Iceberg schema."""
    from pyiceberg.schema import Schema as IcebergSchema
    from pyiceberg.types import NestedField, StringType

    iceberg_schema = IcebergSchema(
        NestedField(field_id=1, name="id", field_type=StringType(), required=True)
    )
    schema = Schema(iceberg_schema)
    assert schema.schema == iceberg_schema
