import pathlib
import pytest
from pyiceberg.catalog import load_catalog
from iceberg_evolve.schema import Schema as EvolveSchema
from iceberg_evolve.diff import SchemaDiff, diff_schemas
from schemaworks import JsonSchemaConverter
from pyiceberg.types import BooleanType, StringType


@pytest.fixture(scope="module")
def setup_iceberg_table():
    """
    Creates a test namespace and table in the Hive Iceberg catalog using PyIceberg.
    Yields the catalog and table name for use in tests.
    """
    catalog = load_catalog("hive", **{"type": "hive"})

    namespace = "iceberg_evolve_test"
    table_name = "users"
    full_identifier = f"{namespace}.{table_name}"

    # Ensure the namespace exists
    try:
        catalog.create_namespace(namespace)
    except Exception:
        pass  # already exists
    print("namespaces:", catalog.list_namespaces())
    converter = JsonSchemaConverter()
    converter.load_schema_from_file("./examples/users_current.json")

    # Convert JSON schema to Iceberg schema
    iceberg_schema = converter.to_iceberg_schema()

    # Create table
    try:
        catalog.drop_table(full_identifier)
    except Exception:
        pass

    catalog.create_table(
        identifier=full_identifier,
        schema=iceberg_schema,
        #location="s3a://warehouse/iceberg_evolve_test"
    )
    table = catalog.load_table(full_identifier)
    print("table location:", table.location())

    yield catalog, full_identifier

    # Cleanup after test
    catalog.drop_table(full_identifier)
    try:
        catalog.drop_namespace(namespace)
    except Exception:
        pass


def test_schema_diff_against_catalog(setup_iceberg_table):
    """
    Integration test to verify that SchemaDiff correctly detects added fields
    between a Hive Iceberg table and a schema loaded from JSON.
    """
    _, table_identifier = setup_iceberg_table

    json_path = pathlib.Path("examples/users_new.json")
    assert json_path.exists(), "Missing test schema JSON file."

    json_schema = EvolveSchema.from_file("examples/users_new.json")
    iceberg_schema = EvolveSchema.from_iceberg(table_identifier, catalog="hive")

    diff = SchemaDiff.from_schemas(iceberg_schema, json_schema)
    diff.display()

    assert len(diff.added) == 2
    assert diff.added[0].name == "is_active"
    assert diff.added[0].current_type is None
    assert diff.added[0].new_type == BooleanType()
    assert diff.added[1].name == "email"
    assert diff.added[1].current_type is None
    assert diff.added[1].new_type == StringType()
