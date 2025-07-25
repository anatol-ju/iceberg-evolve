import json
import pathlib
import pytest
from pyiceberg.catalog import load_catalog
from iceberg_evolve.schema import Schema as EvolveSchema
from iceberg_evolve.diff import SchemaDiff
from iceberg_evolve.evolution_operation import RenameColumn, UpdateColumn
from pyiceberg.types import BooleanType, StringType, ListType
from rich.console import Console


# Note: The examples directory must include:
# - users_current.iceberg.json
# - users_new.iceberg.json
# - users_renamed.iceberg.json
# with correct Iceberg-style schema including field-ids


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

    iceberg_schema = EvolveSchema.from_file("./examples/users_current.iceberg.json")

    # Create table
    try:
        catalog.drop_table(full_identifier)
    except Exception:
        pass

    catalog.create_table(
        identifier=full_identifier,
        schema=iceberg_schema.schema,
        # location="s3a://warehouse/iceberg_evolve_test"
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

    assert full_identifier not in catalog.list_tables(namespace)


def test_schema_diff_against_catalog(setup_iceberg_table):
    """
    Integration test to verify that SchemaDiff correctly detects added fields
    between a Hive Iceberg table and a schema loaded from JSON.
    """
    _, table_identifier = setup_iceberg_table

    json_path = pathlib.Path("examples/users_new.iceberg.json")
    assert json_path.exists(), "Missing test schema JSON file."

    json_schema = EvolveSchema.from_file("examples/users_new.iceberg.json")
    iceberg_schema = EvolveSchema.from_iceberg(table_identifier, catalog="hive")
    print(json_schema)
    print(iceberg_schema)

    assert json_schema.schema is not None
    assert iceberg_schema.schema is not None

    diff = SchemaDiff.from_schemas(iceberg_schema, json_schema)
    diff.display()

    assert len(diff.added) == 2
    assert diff.added[0].name == "is_active"
    assert diff.added[0].current_type is None
    assert diff.added[0].new_type == BooleanType()
    # SchemaDiff considers nested fields as well
    assert diff.added[1].name == "metadata.used_login"
    assert diff.added[1].current_type is None
    assert diff.added[1].new_type == StringType()


def test_schema_diff_detects_nested_additions(setup_iceberg_table):
    """
    Integration test to verify that SchemaDiff correctly detects additions in nested structures.
    """
    _, table_identifier = setup_iceberg_table

    new_schema = EvolveSchema.from_file("examples/users_new.iceberg.json")
    original_schema = EvolveSchema.from_iceberg(table_identifier, catalog="hive")

    diff = SchemaDiff.from_schemas(original_schema, new_schema)
    diff.display()

    nested_adds = [field for field in diff.added if "." in field.name]
    assert any("metadata.used_login" == field.name for field in nested_adds), "Expected nested field 'metadata.used_login' not found."
    assert all(field.current_type is None for field in nested_adds)


def test_schema_diff_detects_renames_by_id(setup_iceberg_table):
    """
    Integration test to verify that SchemaDiff detects field renames
    by field ID, even if the name changes.
    """
    _, table_identifier = setup_iceberg_table

    # Example file where one field is renamed but has the same ID
    renamed_path = pathlib.Path("examples/users_renamed.iceberg.json")
    assert renamed_path.exists(), "Missing renamed test schema JSON file."

    new_schema = EvolveSchema.from_file("examples/users_new.iceberg.json")
    original_schema = EvolveSchema.from_iceberg(table_identifier, catalog="hive")

    diff = SchemaDiff.from_schemas(original_schema, new_schema)
    diff.display()

    assert len(diff.added) == 2
    assert len(diff.removed) == 2
    assert any(op.change == "renamed" for op in diff.changed)


def test_schema_diff_all_operation_types(setup_iceberg_table):
    """
    Integration test to verify that SchemaDiff can detect all supported operations:
    add, drop, update, rename, move, and union.
    """
    _, table_identifier = setup_iceberg_table

    json_path = pathlib.Path("examples/users_new.iceberg.json")
    assert json_path.exists(), "Missing schema JSON for all operations."

    new_schema = EvolveSchema.from_file(str(json_path))
    original_schema = EvolveSchema.from_iceberg(table_identifier, catalog="hive")

    diff = SchemaDiff.from_schemas(original_schema, new_schema)
    diff.display()

    # Basic assumptions for this test case
    assert any(op.name == "is_active" for op in diff.added), "Expected 'is_active' to be added"
    assert any(op.name == "comments" for op in diff.removed), "Expected 'comments' to be removed"
    assert any(op.name == "signup_date" and op.current_type != op.new_type for op in diff.changed), "Expected 'signup_date' type to change"
    assert any(op.name == "email" and hasattr(op, "previous_name") for op in diff.changed), "Expected 'email' to be renamed"
    assert any(op.name == "metadata.login_attempts" and isinstance(op.new_type, ListType) for op in diff.changed), "Expected 'metadata.login_attempts' to be updated to struct"

    # Validate EvolutionOperation output
    from iceberg_evolve.evolution_operation import AddColumn, DropColumn, UpdateColumn, RenameColumn, MoveColumn
    ops = diff.to_evolution_operations()

    assert any(isinstance(op, AddColumn) and op.name == "is_active" for op in ops)
    assert any(isinstance(op, DropColumn) and op.name == "comments" for op in ops)
    assert any(isinstance(op, UpdateColumn) and op.name == "signup_date" for op in ops)
    assert any(isinstance(op, RenameColumn) and op.name == "email_address" and op.target == "email" for op in ops)
    assert any(isinstance(op, MoveColumn) and op.name == "username" for op in ops)


def test_schema_diff_no_op(setup_iceberg_table):
    """
    Integration test to verify that SchemaDiff detects no changes when the schemas are identical.
    """
    _, full_identifier = setup_iceberg_table

    original_schema = EvolveSchema.from_iceberg(full_identifier, catalog="hive")
    new_schema = EvolveSchema.from_file("examples/users_current.iceberg.json")
    print(original_schema.schema, new_schema.schema)

    diff = SchemaDiff.from_schemas(original_schema, new_schema)
    diff.display()

    assert len(diff.added) == 0
    assert len(diff.removed) == 0
    assert len(diff.changed) == 0

@pytest.mark.parametrize("bad_schema", [
    {
        # unknown type
        "type": "struct",
        "schema-id": 1,
        "fields": [{"id": 1, "name": "foo", "required": True, "type": "not_a_type"}]
    },
    {
        # missing field ID
        "type": "struct",
        "schema-id": 1,
        "fields": [{"name": "foo", "required": True, "type": "string"}]
    },
    {
        # missing 'fields' key entirely
        "type": "struct",
        "schema-id": 1
    },
    {
        # list without element-id
        "type": "struct",
        "schema-id": 1,
        "fields": [{
            "id": 1,
            "name": "bad_list",
            "required": True,
            "type": {
                "type": "list",
                "element": "string"
                # missing "element-id"
            }
        }]
    }
])
def test_malformed_schemas_fail(tmp_path, bad_schema):
    """
    Test that malformed schemas raise an exception when loaded.
    """
    from iceberg_evolve.exceptions import SchemaParseError

    file_path = tmp_path / "invalid.iceberg.json"
    with file_path.open("w") as f:
        json.dump(bad_schema, f)

    with pytest.raises(SchemaParseError) as exc_info:
        EvolveSchema.from_file(str(file_path))
        assert "Failed to parse schema" in str(exc_info.value)

    # Delete file after test
    file_path.unlink(missing_ok=True)
    assert not file_path.exists(), "Temporary file should be deleted after test."


def test_schema_diff_union_behavior():
    """
    Test that SchemaDiff handles union-like changes correctly, such as added fields
    that may not match existing IDs but are type-compatible.
    """
    old = EvolveSchema.from_file("examples/users_current.iceberg.json")
    new = EvolveSchema.from_file("examples/users_union_candidate.iceberg.json")  # <- new fields, different IDs

    diff = SchemaDiff.from_schemas(old, new)
    diff.display()

    assert any(fc.name == "some_field" and fc.change == "added" for fc in diff.added)

def test_schema_diff_rename_and_type_change():
    """
    Test that SchemaDiff correctly handles a rename + type change on the same field ID.
    """
    original = EvolveSchema.from_file("examples/users_current.iceberg.json")
    modified = EvolveSchema.from_file("examples/users_renamed_and_changed.iceberg.json")

    diff = SchemaDiff.from_schemas(original, modified)
    diff.display()

    assert any(fc.change == "renamed" and fc.previous_name == "old_field" for fc in diff.changed)
    assert any(fc.change == "type_changed" and fc.name == "new_field" for fc in diff.changed)

    # Optional: validate operations list too
    ops = diff.to_evolution_operations()
    assert any(isinstance(op, RenameColumn) and op.name == "old_field" and op.target == "new_field" for op in ops)
    assert any(isinstance(op, UpdateColumn) and op.name == "new_field" for op in ops)

def test_schema_diff_display_output():
    """
    Test that the .display() output is formatted correctly using Rich.
    """
    old = EvolveSchema.from_file("examples/users_current.iceberg.json")
    new = EvolveSchema.from_file("examples/users_new.iceberg.json")

    diff = SchemaDiff.from_schemas(old, new)

    console = Console(record=True)
    diff.display(console)
    output = console.export_text()

    assert "ADDED:" in output
    assert "REMOVED:" not in output  # or adjust based on your schema
    assert "- is_active:" in output

def test_apply_evolution_ops_round_trip(setup_iceberg_table):
    """
    Apply evolution operations to the original schema and verify it becomes the expected schema.
    """
    original = EvolveSchema.from_file("examples/users_current.iceberg.json")
    expected = EvolveSchema.from_file("examples/users_new.iceberg.json")

    catalog, table_identifier = setup_iceberg_table
    table = catalog.load_table(table_identifier)

    evolved_schema = original.evolve(new=expected, table=table, allow_breaking=True)

    new_diff = SchemaDiff.from_schemas(evolved_schema, expected)

    assert new_diff.added == []
    assert new_diff.removed == []
    assert new_diff.changed == []
