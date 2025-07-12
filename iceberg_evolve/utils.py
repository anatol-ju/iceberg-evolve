import re
from uuid import uuid4

from pyiceberg.types import (
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
    TimestampType
)

# mapping for primitive types
_PRIMITIVES = {
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
    "timestamp": TimestampType()
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

def parse_sql_type(type_str: str) -> IcebergType:
    """
    Parse a SQL-style type string into a PyIceberg IcebergType.
    Supports:
      - decimal(p, s)
      - array<inner>
      - map<key, value>
      - struct<field: type, ...>
      - primitive types (string, int, long, float, double, boolean, date, timestamp)
    """
    s = type_str.strip()
    ls = s.lower()

    # decimal(p, s)
    dec = re.match(r"decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)", ls)
    if dec:
        precision, scale = map(int, dec.groups())
        return DecimalType(precision, scale)

    # array<inner>
    if ls.startswith("array<") and ls.endswith(">"):
        inner = s[len("array<"):-1]
        elem = parse_sql_type(inner)
        return ListType(element_id=int(uuid4().int % (1 << 31)), element_type=elem)

    # map<key, value>
    if ls.startswith("map<") and ls.endswith(">"):
        inner = s[len("map<"):-1]
        key_str, val_str = split_top_level(inner, sep=",")
        key = parse_sql_type(key_str.strip())
        val = parse_sql_type(val_str.strip())
        # generate unique IDs for key and value
        key_id = int(uuid4().int % (1 << 31))
        value_id = int(uuid4().int % (1 << 31))
        return MapType(
            key_id=key_id,
            key_type=key,
            value_id=value_id,
            value_type=val
        )

    # struct<...>
    if ls.startswith("struct<") and ls.endswith(">"):
        inner = s[len("struct<"):-1]
        field_specs = split_top_level(inner, sep=",")
        fields = []
        for spec in field_specs:
            name, typ = spec.split(":", 1)
            fields.append(NestedField(
                field_id=int(uuid4().int % (1 << 31)),
                name=name.strip(),
                field_type=parse_sql_type(typ.strip()),
                required=False
            ))
        return StructType(*fields)

    # primitive
    prim = _PRIMITIVES.get(ls)
    if prim is not None:
        return prim

    raise ValueError(f"Unsupported type string '{type_str}'")

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
        return f"array<{clean_type_str(typ.element_type)}>"
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
