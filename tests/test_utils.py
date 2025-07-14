import pytest
from pyiceberg.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
    TimestampType
)

from iceberg_evolve.utils import (
    clean_type_str,
    is_narrower_than,
    parse_sql_type,
    split_top_level
)


def test_split_top_level_simple():
    """Test that split_top_level splits a basic comma-separated string correctly."""
    s = "a,b,c"
    result = split_top_level(s, sep=",")
    assert result == ["a", "b", "c"]

def test_split_top_level_nested():
    """Test that split_top_level ignores commas inside nested angle brackets."""
    s = "key1: value1, key2: struct<inner1: int, inner2: string>, key3: boolean"
    result = split_top_level(s, sep=",")
    expected = [
        "key1: value1",
        " key2: struct<inner1: int, inner2: string>",
        " key3: boolean"
    ]
    assert result == expected

@pytest.mark.parametrize("type_str, expected_type", [
    ("string", StringType),
    ("int", IntegerType),
    ("integer", IntegerType),
    ("long", LongType),
    ("float", FloatType),
    ("double", DoubleType),
    ("boolean", BooleanType),
    ("bool", BooleanType),
    ("date", DateType),
    ("timestamp", TimestampType),
])
def test_parse_primitive_types(type_str, expected_type):
    """Test that parse_sql_type returns the correct Iceberg type for known primitive aliases."""
    t = parse_sql_type(type_str)
    assert isinstance(t, expected_type)

def test_parse_decimal():
    """Test that parse_sql_type correctly parses decimal types with precision and scale."""
    t = parse_sql_type("decimal(5, 2)")
    assert isinstance(t, DecimalType)
    assert t.precision == 5
    assert t.scale == 2

def test_parse_array_of_primitives():
    """Test that parse_sql_type returns a ListType when parsing array of primitives."""
    t = parse_sql_type("array<integer>")
    assert isinstance(t, ListType)
    assert isinstance(t.element_type, IntegerType)

def test_parse_map():
    """Test that parse_sql_type returns a MapType with correct key and value types."""
    t = parse_sql_type("map<string, integer>")
    assert isinstance(t, MapType)
    assert isinstance(t.key_type, StringType)
    assert isinstance(t.value_type, IntegerType)

def test_parse_struct():
    """Test that parse_sql_type returns a StructType with expected field names and types."""
    t = parse_sql_type("struct<foo: string, bar: int>")
    assert isinstance(t, StructType)
    fields = t.fields
    names = [f.name for f in fields]
    types = [f.field_type for f in fields]
    assert names == ["foo", "bar"]
    assert isinstance(types[0], StringType)
    assert isinstance(types[1], IntegerType)

def test_parse_nested_struct():
    """Test that parse_sql_type correctly parses nested StructTypes."""
    t = parse_sql_type("struct<outer: struct<inner: boolean>>")
    assert isinstance(t, StructType)
    outer_field = t.fields[0]
    assert outer_field.name == "outer"
    nested = outer_field.field_type
    assert isinstance(nested, StructType)
    inner_field = nested.fields[0]
    assert inner_field.name == "inner"
    assert isinstance(inner_field.field_type, BooleanType)

def test_parse_array_of_struct():
    """Test that parse_sql_type returns a ListType with a StructType element."""
    t = parse_sql_type("array<struct<foo: string, bar: int>>")
    assert isinstance(t, ListType)
    element = t.element_type
    assert isinstance(element, StructType)
    names = [f.name for f in element.fields]
    assert names == ["foo", "bar"]

@pytest.mark.parametrize("type_str", [
    "unknown",
    "map<unknown, int>",
    "array<unknown>",
    "struct<foo: unknown>",
])
def test_parse_sql_type_error(type_str):
    """Test that parse_sql_type raises ValueError for unsupported or unknown type strings."""
    with pytest.raises(ValueError):
        parse_sql_type(type_str)

def test_is_narrower_than_cases():
    """Test the is_narrower_than function for valid numeric widening and invalid cases."""
    assert is_narrower_than(IntegerType(), LongType())
    assert is_narrower_than(IntegerType(), FloatType())
    assert is_narrower_than(IntegerType(), DoubleType())
    assert is_narrower_than(IntegerType(), DecimalType(10, 2))
    assert is_narrower_than(LongType(), DoubleType())
    assert is_narrower_than(FloatType(), DecimalType(10, 2))
    assert is_narrower_than(DoubleType(), DecimalType(10, 2))
    assert not is_narrower_than(StringType(), IntegerType())
    assert not is_narrower_than(DoubleType(), IntegerType())

def test_clean_type_str_primitive():
    """Test clean_type_str returns lowercase string names for primitive types."""
    assert clean_type_str(StringType()) == "string"
    assert clean_type_str(BooleanType()) == "boolean"

def test_clean_type_str_list():
    """Test clean_type_str formats Iceberg ListType correctly with nested type string."""
    t = ListType(element_id=1, element_type=StringType())
    assert clean_type_str(t) == "array<string>"

def test_clean_type_str_map():
    """Test clean_type_str formats Iceberg MapType with key/value type strings."""
    t = MapType(
        key_id=1, key_type=StringType(),
        value_id=2, value_type=IntegerType()
    )
    assert clean_type_str(t) == "map<string, int>"

def test_clean_type_str_struct():
    """Test clean_type_str correctly formats StructType with optional and required fields."""
    fields = [
        NestedField(field_id=1, name="name", field_type=StringType(), required=True),
        NestedField(field_id=2, name="age", field_type=IntegerType(), required=False)
    ]
    t = StructType(*fields)
    assert clean_type_str(t) == "struct<name: string, age: optional int>"
