import pytest
from iceberg_evolve.schema import Schema

def test_schema_normalization():
    fields = [
        {"name": "Email", "type": "string"},
        {"name": "id", "type": "int"}
    ]
    schema = Schema(fields=fields)
    assert schema.fields == [
        {"name": "email", "type": "string"},
        {"name": "id", "type": "int"}
    ]

def test_missing_name_raises():
    fields = [{"type": "int"}]
    with pytest.raises(ValueError):
        Schema(fields=fields)

def test_missing_type_raises():
    fields = [{"name": "foo"}]
    with pytest.raises(ValueError):
        Schema(fields=fields)

def test_to_dict():
    fields = [{"name": "id", "type": "int"}]
    schema = Schema(fields=fields)
    assert schema.to_dict() == {"fields": [{"name": "id", "type": "int"}]}
