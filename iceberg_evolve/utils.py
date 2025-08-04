import re

from pyiceberg.table import Table
from pyiceberg.table.update.schema import UpdateSchema
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IcebergType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
    TimestampType,
    TimeType
)
from rich.tree import Tree

# consolidated mapping for primitive types used in both SQL and JSON parsing
_PRIMITIVE_TYPES = {
    "string": StringType(),
    "int": IntegerType(),
    "integer": IntegerType(),
    "long": LongType(),
    "float": FloatType(),
    "double": DoubleType(),
    # decimal is handled separately
    "boolean": BooleanType(),
    "bool": BooleanType(),
    "date": DateType(),
    "time": TimeType(),
    "timestamp": TimestampType(),
    "binary": BinaryType(),
}


def split_top_level(s: str, sep: str = ",") -> list[str]:
    """
    Split a string on sep but only at top-level (ignoring separators inside <>).
    """
    parts, buf, depth = [], "", 0
    for ch in s:
        if ch == "<":
            depth += 1
        elif ch == ">":
            depth -= 1
        if ch == sep and depth == 0:
            parts.append(buf)
            buf = ""
        else:
            buf += ch
    if buf:
        parts.append(buf)
    return parts

def parse_sql_type_with_ids(type_str: str, allocator: 'IDAllocator') -> IcebergType:
    s = type_str.strip()
    ls = s.lower()

    dec = re.match(r"decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)", ls)
    if dec:
        precision, scale = map(int, dec.groups())
        return DecimalType(precision, scale)

    if ls.startswith("array<") and ls.endswith(">"):
        inner = s[len("array<"):-1]
        elem = parse_sql_type_with_ids(inner, allocator)
        return ListType(element_id=allocator.next(), element_type=elem)

    if ls.startswith("map<") and ls.endswith(">"):
        inner = s[len("map<"):-1]
        key_str, val_str = split_top_level(inner, sep=",")
        key = parse_sql_type_with_ids(key_str.strip(), allocator)
        val = parse_sql_type_with_ids(val_str.strip(), allocator)
        return MapType(
            key_id=allocator.next(),
            key_type=key,
            value_id=allocator.next(),
            value_type=val
        )

    if ls.startswith("struct<") and ls.endswith(">"):
        inner = s[len("struct<"):-1]
        field_specs = split_top_level(inner, sep=",")
        fields = []
        for spec in field_specs:
            name, typ = spec.split(":", 1)
            fields.append(NestedField(
                field_id=allocator.next(),
                name=name.strip(),
                field_type=parse_sql_type_with_ids(typ.strip(), allocator),
                required=False
            ))
        return StructType(*fields)

    prim = _PRIMITIVE_TYPES.get(ls)
    if prim is not None:
        return prim

    raise ValueError(f"Unsupported type string '{type_str}'")

# Updated existing function to maintain interface compatibility
def parse_sql_type(type_str: str) -> IcebergType:
    return parse_sql_type_with_ids(type_str, allocator=IDAllocator())

def is_narrower_than(first: IcebergType, second: IcebergType) -> bool:
    """
    Return True if 'first' can be promoted to 'second' without loss of information.
    Numeric widening allowed:
      int -> long, float, double, decimal
      long -> float, double, decimal
      float -> double, decimal
      double -> decimal
    """
    if isinstance(first, IntegerType) and isinstance(second, (LongType, FloatType, DoubleType, DecimalType)):
        return True
    if isinstance(first, LongType) and isinstance(second, (FloatType, DoubleType, DecimalType)):
        return True
    if isinstance(first, FloatType) and isinstance(second, (DoubleType, DecimalType)):
        return True
    if isinstance(first, DoubleType) and isinstance(second, DecimalType):
        return True
    return False

def clean_type_str(typ: IcebergType) -> str:
    """
    Return a simplified string version of an IcebergType, hiding field/element IDs.
    Useful for display or serialization where IDs are internal noise.
    """
    if isinstance(typ, ListType):
        return f"list<{clean_type_str(typ.element_type)}>"
    elif isinstance(typ, MapType):
        return f"map<{clean_type_str(typ.key_type)}, {clean_type_str(typ.value_type)}>"
    elif isinstance(typ, StructType):
        parts = []
        for f in typ.fields:
            optional = "optional " if not f.required else ""
            parts.append(f"{f.name}: {optional}{clean_type_str(f.field_type)}")
        return f"struct<{', '.join(parts)}>"
    else:
        return str(typ).lower()

class IDAllocator:
    def __init__(self):
        self.counter = 1

    def next(self) -> int:
        val = self.counter
        self.counter += 1
        return val

def convert_json_to_iceberg_field(
    name: str,
    spec: dict,
    allocator: IDAllocator,
    required_fields: set[str],
) -> NestedField:
    """
    Convert a JSON schema field spec to an Iceberg NestedField.
    Handles primitive types, arrays, maps, and nested structs.

    Args:
        name (str): The name of the field.
        spec (dict): The JSON schema specification for the field (contains 'properties', 'type', 'items').
        allocator (IDAllocator): An IDAllocator instance to assign field IDs.
        required_fields (set[str]): Set of required field names for ID allocation.

    Returns:
        NestedField: The corresponding Iceberg NestedField.
    """
    json_type = spec.get("type")
    field_id = allocator.next()
    required = name in required_fields

    if json_type == "object":
        if "properties" in spec:
            props = spec["properties"]
            fields = [
                convert_json_to_iceberg_field(child_name, child_spec, allocator, required_fields)
                for child_name, child_spec in props.items()
            ]
            return NestedField(
                field_id=field_id,
                name=name,
                field_type=StructType(*fields),
                required=required
            )
        elif "additionalProperties" in spec:
            value_spec = spec["additionalProperties"]
            value_field = convert_json_to_iceberg_field(name + "_value", value_spec, allocator, required_fields)
            return NestedField(
                field_id=field_id,
                name=name,
                field_type=MapType(
                    key_id=allocator.next(),
                    key_type=StringType(),
                    value_id=allocator.next(),
                    value_type=value_field.field_type,
                    value_required=True
                ),
                required=required
            )
        else:
            raise ValueError(f"Object field '{name}' must define either 'properties' or 'additionalProperties'.")

    elif json_type == "array":
        items = spec.get("items")
        if not isinstance(items, dict):
            raise ValueError(f"Array field '{name}' must have 'items' defined.")
        element_field = convert_json_to_iceberg_field(name + "_element", items, allocator, required_fields)
        return NestedField(
            field_id=field_id,
            name=name,
            field_type=ListType(
                element_id=allocator.next(),
                element_type=element_field.field_type,
                element_required=True  # assume required
            ),
            required=required
        )

    elif json_type == "map":
        props = spec.get("properties", {})
        key_spec = props.get("key")
        val_spec = props.get("value")
        if not key_spec or not val_spec:
            raise ValueError(f"Map field '{name}' must have 'key' and 'value' under 'properties'.")
        key_type = _PRIMITIVE_TYPES[key_spec["type"]]
        value_field = convert_json_to_iceberg_field(name + "_value", val_spec, allocator, required_fields)
        return NestedField(
            field_id=field_id,
            name=name,
            field_type=MapType(
                key_id=allocator.next(),
                key_type=key_type,
                value_id=allocator.next(),
                value_type=value_field.field_type,
                value_required=True
            ),
            required=required
        )

    else:
        iceberg_type = _PRIMITIVE_TYPES.get(json_type)
        if iceberg_type is None:
            raise ValueError(f"Unsupported primitive type '{json_type}' in JSON schema.")
        return NestedField(
            field_id=field_id,
            name=name,
            field_type=iceberg_type,
            required=required
        )


def render_type(node: Tree, tp: IcebergType):
    """
    Render an IcebergType into a rich Tree structure, adding nodes for each field and type.

    Args:
        node (Tree): The root Tree node to add the type structure to.
        tp (IcebergType): The Iceberg type to render.
    """
    if isinstance(tp, StructType):
        for field in tp.fields:
            required = "required" if field.required else ""
            field_type = field.field_type
            # Only render primitive types as a single line and continue
            if not isinstance(field_type, (StructType, ListType, MapType)):
                node.add(f"{field.name}: {str(field_type)}{(' ' + required) if required else ''}")
                continue
            if isinstance(field_type, StructType):
                child = node.add(f"{field.name}: struct{(' ' + required) if required else ''}")
                render_type(child, field_type)
            elif isinstance(field_type, ListType):
                elem_type = field_type.element_type
                if isinstance(elem_type, StructType):
                    list_node = node.add(f"{field.name}: list<struct>{(' ' + required) if required else ''}")
                    render_type(list_node, elem_type)
                else:
                    node.add(f"{field.name}: list<{str(elem_type)}>{(' ' + required) if required else ''}")
            elif isinstance(field_type, MapType):
                map_node = node.add(f"{field.name}: map{(' ' + required) if required else ''}")
                key_node = map_node.add("key")
                render_type(key_node, field_type.key_type)
                value_node = map_node.add("value")
                render_type(value_node, field_type.value_type)
    elif isinstance(tp, ListType):
        # If required/optional annotation is added later, it should go after the type as well.
        tree = node.add("list<struct>" if isinstance(tp.element_type, StructType) else f"list<{str(tp.element_type)}>")
        if isinstance(tp.element_type, StructType):
            render_type(tree, tp.element_type)
    elif isinstance(tp, MapType):
        map_node = node.add("map")
        key_node = map_node.add("key")
        render_type(key_node, tp.key_type)
        value_node = map_node.add("value")
        render_type(value_node, tp.value_type)
    else:
        node.add(str(tp))


def type_to_tree(label: str, tp: IcebergType) -> Tree:
    if not isinstance(tp, (StructType, ListType, MapType)):
        return Tree(f"{label}: {clean_type_str(tp)}")

    root_label = f"{label}: list" if isinstance(tp, ListType) else f"{label}: struct" if isinstance(tp, StructType) else f"{label}: {str(tp)}"
    root = Tree(root_label)
    render_type(root, tp)
    return root


def canonicalize_type(t: IcebergType) -> IcebergType:
    """
    Return a canonicalized version of an Iceberg type.
    Strips docstrings and reorders struct fields by field_id.
    This function is used to ensure that Iceberg types are compared in a consistent manner.
    """
    if isinstance(t, StructType):
        sorted_fields = sorted(t.fields, key=lambda f: f.field_id)
        new_fields = []
        for f in sorted_fields:
            new_field_type = canonicalize_type(f.field_type)
            new_fields.append(NestedField(
                field_id=f.field_id,
                name=f.name,
                field_type=new_field_type,
                required=f.required
            ))
        return StructType(*new_fields)

    elif isinstance(t, ListType):
        return ListType(
            element_id=t.element_id,
            element_required=t.element_required,
            element_type=canonicalize_type(t.element_type)
        )

    elif isinstance(t, MapType):
        return MapType(
            key_id=t.key_id,
            key_type=canonicalize_type(t.key_type),
            value_id=t.value_id,
            value_type=canonicalize_type(t.value_type),
            value_required=t.value_required
        )

    else:
        return t


def types_equivalent(a: IcebergType, b: IcebergType) -> bool:
    """
    Compare two Iceberg types for structural equivalence,
    ignoring field order and non-essential metadata like docs.
    """
    ca = canonicalize_type(a)
    cb = canonicalize_type(b)
    return ca == cb


def open_update_context(table: Table, allow_incompatible_changes: bool) -> UpdateSchema:
    """
    Open an update context for the given table, allowing for schema changes.
    This function abstracts the update context creation to handle both
    PyIceberg versions that support `allow_incompatible_changes` and those that do not.

    Args:
        table (Table): The Iceberg table to update.
        allow_incompatible_changes (bool): Whether to allow incompatible schema changes.

    Returns:
        UpdateSchema: The context for updating the table schema.
    """
    try:
        return table.update_schema(allow_incompatible_changes=allow_incompatible_changes)
    except TypeError:
        return table.update_schema()
