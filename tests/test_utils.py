

import pytest
from iceberg_evolve.utils import split_top_level, parse_sql_type
from pyiceberg.types import (
    StringType, IntegerType, LongType, FloatType, DoubleType,
    BooleanType, DateType, TimestampType, DecimalType,
    ListType, MapType, StructType, NestedField
)

def test_split_top_level_simple():
    s = "a,b,c"
    result = split_top_level(s, sep=",")
    assert result == ["a", "b", "c"]

def test_split_top_level_nested():
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
    t = parse_sql_type(type_str)
    assert isinstance(t, expected_type)

def test_parse_decimal():
    t = parse_sql_type("decimal(5, 2)")
    assert isinstance(t, DecimalType)
    assert t.precision == 5
    assert t.scale == 2

def test_parse_array_of_primitives():
    t = parse_sql_type("array<integer>")
    assert isinstance(t, ListType)
    assert isinstance(t.element_type, IntegerType)

def test_parse_map():
    t = parse_sql_type("map<string, integer>")
    assert isinstance(t, MapType)
    assert isinstance(t.key_type, StringType)
    assert isinstance(t.value_type, IntegerType)

def test_parse_struct():
    t = parse_sql_type("struct<foo: string, bar: int>")
    assert isinstance(t, StructType)
    fields = t.fields
    names = [f.name for f in fields]
    types = [f.field_type for f in fields]
    assert names == ["foo", "bar"]
    assert isinstance(types[0], StringType)
    assert isinstance(types[1], IntegerType)

def test_parse_nested_struct():
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
    with pytest.raises(ValueError):
        parse_sql_type(type_str)
