import re

from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.types import (
    DecimalType,
    IcebergType,
    ListType,
    MapType,
    NestedField,
    StructType
)

from iceberg_evolve.utils import _PRIMITIVE_TYPES

class IcebergSchemaJSONSerializer:
    """
    Serializer/deserializer for PyIceberg Schema objects to/from Iceberg's JSON schema format.

    The JSON schema format follows Iceberg's metadata specification:

    - Top-level schema object:
    ```
      {
        "type": "struct",
        "schema-id": <int>,
        "fields": [ <field>, ... ]
      }
    ```

    - Each field object contains:
    ```
      {
        "id": <int>,               # Unique field identifier
        "name": <string>,          # Field name
        "required": <bool>,        # Whether the field is required
        "type": <type_definition>  # Field type descriptor
      }
    ```

    - A `type_definition` may be:
      * A primitive type string (e.g., "string", "int", "boolean", or "decimal(p, s)")
      * A struct:
        ```
        {
          "type": "struct",
          "fields": [ <field>, ... ]
        }
        ```
      * A list:
        ```
        {
          "type": "list",
          "element-id": <int>,
          "element-required": <bool>,
          "element": <type_definition>
        }
        ```
      * A map:
        ```
        {
          "type": "map",
          "key-id": <int>,
          "key": <type_definition>,
          "value-id": <int>,
          "value-required": <bool>,
          "value": <type_definition>
        }
        ```

    This format matches the schema representation Iceberg writes in its metadata JSON.
    """
    @staticmethod
    def to_dict(schema: IcebergSchema) -> dict:
        """
        Serialize an Iceberg Schema object to a dictionary following Iceberg's JSON schema format.

        Args:
            schema (Schema): The Iceberg schema to serialize.

        Returns:
            dict: A dictionary representation of the schema suitable for JSON output.
        """
        def serialize_field(field: NestedField) -> dict:
            return {
                "id": field.field_id,
                "name": field.name,
                "required": field.required,
                "type": serialize_type(field.field_type),
            }

        def serialize_type(iceberg_type: IcebergType):
            if isinstance(iceberg_type, StructType):
                return {
                    "type": "struct",
                    "fields": [serialize_field(f) for f in iceberg_type.fields],
                }
            elif isinstance(iceberg_type, ListType):
                return {
                    "type": "list",
                    "element-id": iceberg_type.element_id,
                    "element-required": iceberg_type.element_required,
                    "element": serialize_type(iceberg_type.element_type),
                }
            elif isinstance(iceberg_type, MapType):
                return {
                    "type": "map",
                    "key-id": iceberg_type.key_id,
                    "key": serialize_type(iceberg_type.key_type),
                    "value-id": iceberg_type.value_id,
                    "value-required": iceberg_type.value_required,
                    "value": serialize_type(iceberg_type.value_type),
                }
            elif isinstance(iceberg_type, DecimalType):
                return f"decimal({iceberg_type.precision}, {iceberg_type.scale})"
            else:
                return str(iceberg_type).lower()

        return {
            "type": "struct",
            "schema-id": getattr(schema, "schema_id", 0),
            "fields": [serialize_field(f) for f in schema.fields],
        }

    @staticmethod
    def from_dict(data: dict) -> IcebergSchema:
        """
        Deserialize a dictionary in Iceberg JSON schema format into a Schema object.

        Args:
            data (dict): A dictionary representing the Iceberg schema.

        Returns:
            Schema: A pyiceberg Schema object reconstructed from the input.
        """
        def parse_field(field_data: dict) -> NestedField:
            return NestedField(
                field_id=field_data["id"],
                name=field_data["name"],
                field_type=parse_type(field_data["type"]),
                required=field_data["required"]
            )

        def parse_type(type_data):
            if isinstance(type_data, str):
                # primitive or decimal string like "decimal(10, 2)"
                match = re.match(r"decimal\((\d+),\s*(\d+)\)", type_data)
                if match:
                    return DecimalType(int(match.group(1)), int(match.group(2)))
                iceberg_type = _PRIMITIVE_TYPES.get(type_data.lower())
                if not iceberg_type:
                    raise ValueError(f"Unsupported primitive type: {type_data}")
                return iceberg_type

            if isinstance(type_data, dict):
                iceberg_type = type_data.get("type")
                if iceberg_type == "struct":
                    return StructType(*[parse_field(f) for f in type_data["fields"]])
                if iceberg_type == "list":
                    return ListType(
                        element_id=type_data["element-id"],
                        element_required=type_data["element-required"],
                        element_type=parse_type(type_data["element"])
                    )
                if iceberg_type == "map":
                    return MapType(
                        key_id=type_data["key-id"],
                        key_type=parse_type(type_data["key"]),
                        value_id=type_data["value-id"],
                        value_required=type_data["value-required"],
                        value_type=parse_type(type_data["value"])
                    )
            raise ValueError(f"Unsupported type structure: {type_data}.")

        fields = [parse_field(f) for f in data["fields"]]
        return IcebergSchema(*fields, schema_id=data.get("schema-id", 0))
