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
from rich.tree import Tree

from iceberg_evolve.utils import (
    IDAllocator,
    canonicalize_type,
    clean_type_str,
    convert_json_to_iceberg_field,
    is_narrower_than,
    parse_sql_type,
    render_type,
    split_top_level,
    types_equivalent
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
    assert clean_type_str(t) == "list<string>"


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


def test_object_with_properties():
    """Test converting a JSON object with nested properties to StructType."""
    spec = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "age": {"type": "integer"}
        }
    }
    field = convert_json_to_iceberg_field("user", spec, IDAllocator(), required_fields={"user"})
    assert field.name == "user"
    assert isinstance(field.field_type, StructType)
    assert len(field.field_type.fields) == 2


def test_object_with_additional_properties():
    """Test converting a JSON object with additionalProperties to MapType."""
    spec = {
        "type": "object",
        "additionalProperties": {"type": "string"}
    }
    field = convert_json_to_iceberg_field("metadata", spec, IDAllocator(), required_fields=set())
    assert field.name == "metadata"
    assert isinstance(field.field_type, MapType)
    assert isinstance(field.field_type.key_type, StringType)
    assert isinstance(field.field_type.value_type, StringType)


def test_array_of_primitives():
    """Test converting a JSON array of primitive types to ListType."""
    spec = {
        "type": "array",
        "items": {"type": "boolean"}
    }
    field = convert_json_to_iceberg_field("flags", spec, IDAllocator(), required_fields=set())
    assert field.name == "flags"
    assert isinstance(field.field_type, ListType)
    assert isinstance(field.field_type.element_type, BooleanType)


def test_map_with_key_value():
    """Test converting a JSON map field with key/value under properties."""
    spec = {
        "type": "map",
        "properties": {
            "key": {"type": "string"},
            "value": {"type": "integer"}
        }
    }
    field = convert_json_to_iceberg_field("scores", spec, IDAllocator(), required_fields={"scores"})
    assert field.name == "scores"
    assert isinstance(field.field_type, MapType)
    assert isinstance(field.field_type.key_type, StringType)
    assert isinstance(field.field_type.value_type, IntegerType)


def test_primitive_field():
    """Test converting a primitive JSON type to the correct Iceberg type."""
    spec = {"type": "date"}
    field = convert_json_to_iceberg_field("created_at", spec, IDAllocator(), required_fields={"created_at"})
    assert field.name == "created_at"
    assert isinstance(field.field_type, DateType)
    assert field.required is True


def test_object_missing_properties():
    """Test that missing 'properties' or 'additionalProperties' raises an error."""
    spec = {"type": "object"}
    with pytest.raises(ValueError, match="must define either 'properties' or 'additionalProperties'"):
        convert_json_to_iceberg_field("invalid_obj", spec, IDAllocator(), required_fields=set())


def test_array_missing_items():
    """Test that missing 'items' in an array raises an error."""
    spec = {"type": "array"}
    with pytest.raises(ValueError, match="must have 'items' defined"):
        convert_json_to_iceberg_field("invalid_arr", spec, IDAllocator(), required_fields=set())


def test_map_missing_key_or_value():
    """Test that a map field missing 'key' or 'value' raises an error."""
    spec = {
        "type": "map",
        "properties": {
            "key": {"type": "string"}
        }
    }
    with pytest.raises(ValueError, match="must have 'key' and 'value' under 'properties'"):
        convert_json_to_iceberg_field("invalid_map", spec, IDAllocator(), required_fields=set())


def test_unsupported_primitive_type():
    """Test that unsupported primitive types raise an error."""
    spec = {"type": "uuid"}
    with pytest.raises(ValueError, match="Unsupported primitive type"):
        convert_json_to_iceberg_field("id", spec, IDAllocator(), required_fields=set())

def test_render_primitive_field():
    """Test rendering of a struct containing only primitive fields."""
    struct = StructType(
        NestedField(1, "name", StringType(), required=True),
        NestedField(2, "age", IntegerType(), required=False)
    )
    tree = Tree("root")
    render_type(tree, struct)
    rendered = [child.label for child in tree.children]
    assert rendered == ["name: string required", "age: int"]

def test_render_nested_struct():
    """Test rendering of a struct with a nested struct field."""
    nested = StructType(
        NestedField(2, "score", DoubleType(), required=True)
    )
    struct = StructType(
        NestedField(1, "exam", nested, required=True)
    )
    tree = Tree("root")
    render_type(tree, struct)
    labels = [child.label for child in tree.children]
    assert labels == ["exam: struct required"]
    nested_labels = [child.label for child in tree.children[0].children]
    assert nested_labels == ["score: double required"]

def test_render_list_of_primitives():
    """Test rendering of a list of primitive types."""
    struct = StructType(
        NestedField(1, "tags", ListType(element_id=2, element_type=StringType(), element_required=True), required=True)
    )
    tree = Tree("root")
    render_type(tree, struct)
    labels = [child.label for child in tree.children]
    assert labels == ["tags: list<string> required"]

def test_render_list_of_structs():
    """Test rendering of a list of structs."""
    element = StructType(
        NestedField(2, "label", StringType(), required=False)
    )
    struct = StructType(
        NestedField(1, "items", ListType(element_id=2, element_type=element, element_required=True), required=True)
    )
    tree = Tree("root")
    render_type(tree, struct)
    labels = [child.label for child in tree.children]
    assert labels == ["items: list<struct> required"]
    nested_labels = [child.label for child in tree.children[0].children]
    assert nested_labels == ["label: string"]

def test_render_map_field():
    """Test rendering of a map field with primitive key and value types."""
    struct = StructType(
        NestedField(
            1,
            "metadata",
            MapType(
                key_id=2,
                key_type=StringType(),
                value_id=3,
                value_type=BooleanType(),
                value_required=True
            ),
            required=False
        )
    )
    tree = Tree("root")
    render_type(tree, struct)
    labels = [child.label for child in tree.children]
    assert labels == ["metadata: map"]
    keys = [c.label for c in tree.children[0].children]
    assert keys == ["key", "value"]
    key_types = [c.children[0].label for c in tree.children[0].children]
    assert key_types == ["string", "boolean"]

def test_render_map_with_struct_value():
    """Test rendering of a map field with struct as value."""
    value_struct = StructType(
        NestedField(3, "flag", BooleanType(), required=True)
    )
    struct = StructType(
        NestedField(
            1,
            "details",
            MapType(
                key_id=2,
                key_type=StringType(),
                value_id=4,
                value_type=value_struct,
                value_required=True
            ),
            required=True
        )
    )
    tree = Tree("root")
    render_type(tree, struct)
    assert tree.children[0].label == "details: map required"
    assert tree.children[0].children[0].label == "key"
    assert tree.children[0].children[0].children[0].label == "string"
    assert tree.children[0].children[1].label == "value"
    assert tree.children[0].children[1].children[0].label == "flag: boolean required"


# Additional test to explicitly cover top-level ListType rendering
def test_render_top_level_list_of_structs():
    """Test rendering a top-level ListType of structs using render_type."""
    element_type = StructType(
        NestedField(1, "label", StringType(), required=True)
    )
    list_type = ListType(element_id=2, element_type=element_type, element_required=True)
    tree = Tree("root")
    render_type(tree, list_type)
    assert tree.children[0].label == "list<struct>"
    assert tree.children[0].children[0].label == "label: string required"

def test_render_top_level_map():
    """Test rendering a top-level MapType with primitive key/value types."""
    map_type = MapType(
        key_id=1,
        key_type=StringType(),
        value_id=2,
        value_type=IntegerType(),
        value_required=True
    )
    tree = Tree("root")
    render_type(tree, map_type)
    assert tree.children[0].label == "map"
    assert tree.children[0].children[0].label == "key"
    assert tree.children[0].children[0].children[0].label == "string"
    assert tree.children[0].children[1].label == "value"
    assert tree.children[0].children[1].children[0].label == "int"

def test_type_to_tree_outputs():
    """Test that type_to_tree produces expected tree output for different IcebergType variants."""
    from iceberg_evolve.utils import type_to_tree
    # Primitive
    tree = type_to_tree("foo", StringType())
    assert tree.label == "foo: string"
    assert len(tree.children) == 0

    # Struct
    struct = StructType(
        NestedField(1, "name", StringType(), required=True),
        NestedField(2, "age", IntegerType(), required=False)
    )
    tree = type_to_tree("person", struct)
    assert tree.label == "person: struct"
    assert tree.children[0].label == "name: string required"
    assert tree.children[1].label == "age: int"

    # List
    list_type = ListType(element_id=1, element_type=StringType(), element_required=True)
    tree = type_to_tree("tags", list_type)
    assert tree.label == "tags: list"
    assert tree.children[0].label == "list<string>"

    # Map
    map_type = MapType(
        key_id=1,
        key_type=StringType(),
        value_id=2,
        value_type=BooleanType(),
        value_required=True
    )
    tree = type_to_tree("metadata", map_type)
    assert tree.label == "metadata: map<string, boolean>"
    map_node = tree.children[0]
    assert map_node.label == "map"
    assert map_node.children[0].label == "key"
    assert map_node.children[0].children[0].label == "string"
    assert map_node.children[1].label == "value"
    assert map_node.children[1].children[0].label == "boolean"


def test_canonicalize_type_listtype_with_struct_element():
    """Test canonicalize_type correctly processes a ListType with a nested StructType element."""
    # Create a struct with out-of-order fields
    nested = StructType(
        NestedField(field_id=2, name="b", field_type=StringType(), required=False),
        NestedField(field_id=1, name="a", field_type=IntegerType(), required=True),
    )
    # Wrap in a list type
    lt = ListType(element_id=7, element_type=nested, element_required=True)
    canon = canonicalize_type(lt)

    # The result should still be a ListType
    assert isinstance(canon, ListType)
    assert canon.element_id == 7
    assert canon.element_required is True

    # Its element_type should be a StructType with fields sorted by ID: 1 then 2
    assert isinstance(canon.element_type, StructType)
    ids = [f.field_id for f in canon.element_type.fields]
    names = [f.name for f in canon.element_type.fields]
    assert ids == [1, 2]
    assert names == ["a", "b"]

def test_canonicalize_type_maptype_with_struct_value():
    """Test canonicalize_type correctly processes a MapType with a nested StructType value."""
    # Create a struct with out-of-order fields
    nested = StructType(
        NestedField(field_id=2, name="b", field_type=DoubleType(), required=True),
        NestedField(field_id=1, name="a", field_type=FloatType(), required=False),
    )
    # Wrap in a map type
    mt = MapType(
        key_id=5,
        key_type=BooleanType(),
        value_id=6,
        value_type=nested,
        value_required=False
    )
    canon = canonicalize_type(mt)

    # The result should still be a MapType
    assert isinstance(canon, MapType)
    assert canon.key_id == 5
    assert canon.value_id == 6
    assert canon.value_required is False

    # Its key_type remains primitive
    assert isinstance(canon.key_type, BooleanType)

    # Its value_type should be a StructType with fields sorted by ID: 1 then 2
    assert isinstance(canon.value_type, StructType)
    ids = [f.field_id for f in canon.value_type.fields]
    names = [f.name for f in canon.value_type.fields]
    assert ids == [1, 2]
    assert names == ["a", "b"]

def test_types_equivalent_list_and_map():
    """Test that types_equivalent returns True for structurally identical ListType and MapType."""
    lt1 = ListType(element_id=10, element_type=IntegerType(), element_required=False)
    lt2 = ListType(element_id=10, element_type=IntegerType(), element_required=False)
    assert types_equivalent(lt1, lt2)

    mt1 = MapType(
        key_id=1, key_type=StringType(),
        value_id=2, value_type=LongType(),
        value_required=True
    )
    mt2 = MapType(
        key_id=1, key_type=StringType(),
        value_id=2, value_type=LongType(),
        value_required=True
    )
    assert types_equivalent(mt1, mt2)
