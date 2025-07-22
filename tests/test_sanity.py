from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.types import IntegerType, NestedField, StringType

from iceberg_evolve.diff import SchemaDiff


def test_schema_diff_sanity_check():
    """
    Sanity check to ensure SchemaDiff uses field IDs for comparison, not just names.

    Simulates:
    - One field added (new ID)
    - One field removed (missing in new)
    - One field renamed but same ID (should not count as added/removed)
    """
    # Base schema: has fields id and name
    base = IcebergSchema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType(), required=True),
    )

    # New schema:
    # - field_id=2 renamed from 'name' to 'full_name'
    # - field_id=3 is a new field
    new = IcebergSchema(
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
