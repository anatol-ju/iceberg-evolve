from pyiceberg.catalog import load_catalog

def iceberg_type_to_json_type(iceberg_type: str) -> str:
    mapping = {
        "int": "integer",
        "long": "integer",
        "float": "number",
        "double": "number",
        "boolean": "boolean",
        "string": "string",
        "date": "string",
        "timestamp": "string",
    }
    return mapping.get(iceberg_type.lower(), "string")

def load_table_schema(table_name: str, catalog_name: str = "default", config: dict = None) -> dict:
    """
    Load an Iceberg table schema from a catalog into JSON schema format.

    Args:
        table_name (str): Name of the table (e.g., "db.table").
        catalog_name (str): Name of the catalog (must match your pyiceberg.yaml).
        config (dict): Optional dictionary of config overrides (e.g., programmatic credentials).

    Returns:
        dict: JSON schema-style dictionary with 'properties' and 'required'.
    """
    catalog = load_catalog(catalog_name, **(config or {}))
    table = catalog.load_table(table_name)

    properties = {}
    required = []

    for field in table.schema().fields:
        json_type = iceberg_type_to_json_type(str(field.field_type))
        properties[field.name] = {"type": json_type}
        if not field.optional:
            required.append(field.name)

    return {
        "type": "object",
        "properties": properties,
        "required": required
    }
