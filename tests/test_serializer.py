import pytest

from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.types import (
    BooleanType,
    DecimalType,
    IntegerType,
    ListType,
    MapType,
    NestedField,
    StringType,
    StructType,
)

from iceberg_evolve.serializer import IcebergSchemaJSONSerializer


def test_iceberg_schema_serializer_to_dict_and_from_dict_roundtrip():
    """Test that serializing and deserializing an Iceberg schema returns an equivalent schema."""
    original_schema = StructType(
        NestedField(field_id=1, name="name", field_type=StringType(), required=True),
        NestedField(field_id=2, name="age", field_type=IntegerType(), required=False),
        NestedField(
            field_id=3,
            name="tags",
            field_type=ListType(element_id=4, element_type=StringType(), element_required=True),
            required=False
        ),
        NestedField(
            field_id=5,
            name="properties",
            field_type=MapType(
                key_id=6,
                key_type=StringType(),
                value_id=7,
                value_type=IntegerType(),
                value_required=True
            ),
            required=False
        )
    )
    iceberg_schema = IcebergSchema(*original_schema.fields, schema_id=42)

    serialized = IcebergSchemaJSONSerializer.to_dict(iceberg_schema)
    assert serialized["schema-id"] == 42
    assert serialized["type"] == "struct"
    assert len(serialized["fields"]) == 4

    deserialized = IcebergSchemaJSONSerializer.from_dict(serialized)
    assert isinstance(deserialized, IcebergSchema)
    assert len(deserialized.fields) == 4
    assert deserialized.schema_id == 42


def test_iceberg_schema_serializer_nested_struct():
    """Test that nested StructTypes are correctly serialized and deserialized."""
    nested_struct = StructType(
        NestedField(field_id=2, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=3, name="flag", field_type=BooleanType(), required=False)
    )
    schema = IcebergSchema(
        NestedField(field_id=1, name="user", field_type=nested_struct, required=True)
    )
    serialized = IcebergSchemaJSONSerializer.to_dict(schema)
    assert serialized["fields"][0]["name"] == "user"
    inner = serialized["fields"][0]["type"]
    assert inner["type"] == "struct"
    assert len(inner["fields"]) == 2

    deserialized = IcebergSchemaJSONSerializer.from_dict(serialized)
    user_field = deserialized.find_field("user")
    assert isinstance(user_field.field_type, StructType)
    assert len(user_field.field_type.fields) == 2


def test_iceberg_schema_serializer_decimal_string_format():
    """Test that decimal types are serialized as strings and parsed back correctly."""
    schema = IcebergSchema(
        NestedField(field_id=1, name="amount", field_type=DecimalType(10, 2), required=True)
    )
    serialized = IcebergSchemaJSONSerializer.to_dict(schema)
    field_type = serialized["fields"][0]["type"]
    assert field_type == "decimal(10, 2)"

    deserialized = IcebergSchemaJSONSerializer.from_dict(serialized)
    assert isinstance(deserialized.fields[0].field_type, DecimalType)
    assert deserialized.fields[0].field_type.precision == 10
    assert deserialized.fields[0].field_type.scale == 2


def test_iceberg_schema_serializer_invalid_type_string():
    """Test that unsupported primitive type strings raise ValueError during deserialization."""
    invalid_dict = {
        "type": "struct",
        "schema-id": 1,
        "fields": [
            {"id": 1, "name": "foo", "type": "unsupported", "required": True}
        ]
    }
    with pytest.raises(ValueError, match="Unsupported primitive type"):
        IcebergSchemaJSONSerializer.from_dict(invalid_dict)


def test_iceberg_schema_serializer_invalid_type_structure():
    """Test that non-string and non-dict types raise ValueError during deserialization."""
    invalid_dict = {
        "type": "struct",
        "fields": [
            {"id": 1, "name": "foo", "type": 123, "required": True}
        ]
    }
    with pytest.raises(ValueError, match="Unsupported type structure"):
        IcebergSchemaJSONSerializer.from_dict(invalid_dict)