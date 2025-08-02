import json
import pathlib
import warnings

import pytest
from pyiceberg.catalog import load_catalog
from pyiceberg.types import BooleanType, IntegerType, ListType, NestedField, StringType, TimestampType
from rich.console import Console

from iceberg_evolve.diff import SchemaDiff
from iceberg_evolve.migrate import RenameColumn, UpdateColumn
from iceberg_evolve.schema import Schema as EvolveSchema



# Note: The examples directory must include:
# - users_current.iceberg.json
# - users_new.iceberg.json
# - users_renamed.iceberg.json
# with correct Iceberg-style schema including field-ids


@pytest.mark.integration
def test_schema_diff_sanity_check():
    """
    Sanity check to ensure SchemaDiff uses field IDs for comparison, not just names.

    Simulates:
    - One field added (new ID)
    - One field removed (missing in new)
    - One field renamed but same ID (should not count as added/removed)
    """
    # Base schema: has fields id and name
    base = EvolveSchema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType(), required=True),
    )

    # New schema:
    # - field_id=2 renamed from 'name' to 'full_name'
    # - field_id=3 is a new field
    new = EvolveSchema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="full_name", field_type=StringType(), required=True),
        NestedField(field_id=3, name="age", field_type=IntegerType(), required=False),
    )

    diff = SchemaDiff.from_schemas(base, new)

    # Check that:
    # - id (same name and ID): unchanged
    # - name → full_name (same ID): rename only, not added/removed
    # - age (new field ID): added
    assert len(diff.added) == 1
    assert diff.added[0].name == "age"

    assert len(diff.removed) == 0

    # field renamed by ID should not show up in changed
    assert all(change.name != "name" for change in diff.changed)
    assert [change.name for change in diff.changed] == ["full_name"]


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


@pytest.mark.integration
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


@pytest.mark.integration
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


@pytest.mark.integration
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


@pytest.mark.integration
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
    assert any(op.name == "signup_datetime" and op.current_type != op.new_type for op in diff.changed), "Expected 'signup_datetime' type to change"
    assert any(op.name == "email" and hasattr(op, "previous_name") for op in diff.changed), "Expected 'email' to be renamed"
    assert any(op.name == "metadata.login_attempts" and isinstance(op.new_type, ListType) for op in diff.changed), "Expected 'metadata.login_attempts' to be updated to struct"

    # Validate EvolutionOperation output
    from iceberg_evolve.evolution_operation import AddColumn, DropColumn, UpdateColumn, RenameColumn, MoveColumn
    ops = diff.to_evolution_operations()

    assert any(isinstance(op, AddColumn) and op.name == "is_active" for op in ops)
    assert any(isinstance(op, DropColumn) and op.name == "comments" for op in ops)
    assert any(isinstance(op, UpdateColumn) and op.name == "signup_datetime" for op in ops)
    assert any(isinstance(op, RenameColumn) and op.name == "email_address" and op.target == "email" for op in ops)
    assert any(isinstance(op, MoveColumn) and op.name == "username" for op in ops)


@pytest.mark.integration
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


@pytest.mark.integration
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


@pytest.mark.integration
def test_schema_diff_union_behavior():
    """
    Test that SchemaDiff handles union-like changes correctly, such as added fields
    that may not match existing IDs but are type-compatible.
    """
    old = EvolveSchema.from_file("examples/users_current.iceberg.json")
    new = EvolveSchema.from_file("examples/users_union_candidate.iceberg.json")  # <- new fields, different IDs

    diff = SchemaDiff.union_by_name(old, new)
    diff.display()

    diff = SchemaDiff.union_by_name(old, new)
    assert "new_address" in [f.name for f in diff.added]


@pytest.mark.integration
def test_schema_diff_rename_and_type_change():
    """
    Test that SchemaDiff correctly handles a rename AND type change on the same field ID.
    """
    original = EvolveSchema.from_file("examples/users_current.iceberg.json")
    modified = EvolveSchema.from_file("examples/users_renamed_and_changed.iceberg.json")

    diff = SchemaDiff.from_schemas(original, modified)
    diff.display()

    assert any(fc.change == "renamed" and fc.previous_name == "signup" for fc in diff.changed)
    assert any(fc.change == "type_changed" and fc.name == "signup_ts" for fc in diff.changed)

    # Optional: validate operations list too
    ops = diff.to_evolution_operations()
    assert any(isinstance(op, RenameColumn) and op.name == "signup" and op.target == "signup_ts" for op in ops)
    assert any(isinstance(op, UpdateColumn) and op.name == "signup_ts" for op in ops)


@pytest.mark.integration
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

    assert "ADDED" in output
    assert "REMOVED" in output
    assert "+ is_active:" in output


@pytest.mark.integration
def test_apply_evolution_ops_round_trip(setup_iceberg_table):
    """
    1. Load the “current” and “new” schemas from your JSON files.
    2. Apply evolve(..., allow_breaking=True, return_applied_schema=True).
    3. Capture exactly two UnsupportedSchemaEvolutionWarnings for nested structs.
    4. Assert the live table's field set & types match the expected.
    5. Run a name-based diff and assert only 'metadata' remains in changed.
    """
    # 1) Load schemas
    original = EvolveSchema.from_file("examples/users_current.iceberg.json")
    expected = EvolveSchema.from_file("examples/users_new.iceberg.json")

    catalog, table_id = setup_iceberg_table
    table = catalog.load_table(table_id)

    # 2) Evolve + capture warnings
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        evolved = original.evolve(
            new=expected,
            table=table,
            allow_breaking=True,
            return_applied_schema=True,
            quiet=False
        )

    # 3) Exactly two nested-struct warnings should have been emitted
    assert len(w) == 2
    messages = [str(wi.message) for wi in w]
    assert any("metadata" in msg and "nested" in msg.lower() for msg in messages)
    assert any("metadata.login_attempts" in msg for msg in messages)

    # 4) Spot-check that supported changes really took effect
    evolved_names = {f.name for f in evolved.schema.fields}
    expected_names = {f.name for f in expected.schema.fields}
    # field‐set matches
    assert evolved_names == expected_names

    # rename: signup → signup_datetime
    assert "signup_datetime" in evolved_names and "signup" not in evolved_names
    # type promotion: signup_datetime is timestamp
    fld = evolved.schema.find_field("signup_datetime")
    assert isinstance(fld.field_type, TimestampType)

    # rename: email_address → email
    assert "email" in evolved_names and "email_address" not in evolved_names

    # add: is_active
    assert "is_active" in evolved_names
    # drop: comments
    assert "comments" not in evolved_names

    # move: username after signup_date
    fields = [f.name for f in evolved.schema.fields]
    # 1) username follows signup_date
    idx_signup = fields.index("signup_datetime")
    assert fields[idx_signup + 1] == "username"
    # 2) is_active is last
    assert fields[-1] == "is_active"

    # 5) Name-based diff: only top-level 'metadata' remains unmatched
    post = SchemaDiff.union_by_name(evolved, expected)
    assert post.added   == []
    assert post.removed == []
    assert {fc.name for fc in post.changed} == {"metadata"}
