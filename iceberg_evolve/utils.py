from pyiceberg.types import (
    BooleanType,
    DateType,
    DoubleType,
    IcebergType,
    IntegerType,
    StringType,
    TimestampType,
    LongType,
    DecimalType,
    FloatType,
    StructType,
    NestedField,
    ListType
)
from uuid import uuid4

def from_json_type(type_str: str) -> IcebergType:
    """
    Converts a string-based type name to a PyIceberg type instance.

    Args:
        type_str (str): A string representing the type (e.g., "string", "integer").

    Returns:
        A PyIceberg type instance.

    Raises:
        ValueError: If the type string is unknown.
    """
    type_map = {
        "string": StringType(),
        "integer": IntegerType(),
        "int": IntegerType(),
        "double": DoubleType(),
        "float": DoubleType(),
        "boolean": BooleanType(),
        "bool": BooleanType(),
        "date": DateType(),
        "timestamp": TimestampType(),
    }

    lower = type_str.lower()
    if lower not in type_map:
        raise ValueError(f"Unsupported type: {type_str}. Supported types are: {', '.join(type_map.keys())}")
    return type_map[lower]


def parse_struct_type(type_str: str) -> StructType:
    """
    Parse a struct type string into a PyIceberg StructType.

    Example:
        'struct<foo: string, bar: integer>' -> StructType([...])
    """

    # Remove 'struct<' prefix and trailing '>'
    inner = type_str[len("struct<"):-1]
    fields = []

    for part in inner.split(","):
        name_type = part.strip().split(":")
        if len(name_type) != 2:
            raise ValueError(f"Invalid struct field: {part}")
        name, type_name = name_type[0].strip(), name_type[1].strip()
        fields.append(NestedField(
            field_id=int(uuid4().int % (1 << 31)),
            name=name,
            field_type=from_json_type(type_name),
            required=False
        ))

    return StructType(fields)


def parse_type_str(type_str: str) -> IcebergType | None:
    """
    Parse a JSON-style type string into an IcebergType.
    Supports primitives, structs, and arrays.
    """
    if not type_str:
        return None
    ts = type_str.strip()
    if ts.startswith("struct<") and ts.endswith(">"):
        return parse_struct_type(ts)
    if ts.startswith("array<") and ts.endswith(">"):
        # parse inner type and wrap in ArrayType
        inner = ts[len("array<"):-1]
        inner_type = parse_type_str(inner)
        return ListType(inner_type)
    # primitive
    return from_json_type(ts)


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
