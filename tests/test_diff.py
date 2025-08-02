import sys

import pytest
from pyiceberg.types import (
    IntegerType,
    ListType,
    NestedField,
    StringType,
    StructType
)

from iceberg_evolve.diff import FieldChange, SchemaDiff
from iceberg_evolve.schema import Schema


def make_schema(fields):
    """Helper to wrap a list of fields into a Schema."""
    from pyiceberg.schema import Schema as IcebergSchema
    return Schema(IcebergSchema(*fields))


def test_add_column_diff():
    """Should detect a new field as 'added'."""
    current = make_schema([])
    new = make_schema([
        NestedField(field_id=1, name="name", field_type=StringType(), required=True)
    ])
    diff = SchemaDiff.from_schemas(current, new)
    assert len(diff.added) == 1
    assert diff.added[0].name == "name"
    assert diff.added[0].change == "added"


def test_removed_column_diff():
    """Should detect a removed field as 'removed'."""
    current = make_schema([
        NestedField(field_id=1, name="name", field_type=StringType(), required=True)
    ])
    new = make_schema([])
    diff = SchemaDiff.from_schemas(current, new)
    assert len(diff.removed) == 1
    assert diff.removed[0].name == "name"
    assert diff.removed[0].change == "removed"


def test_renamed_column_diff():
    """Should detect a field with same ID but different name as 'renamed'."""
    current = make_schema([
        NestedField(field_id=1, name="first_name", field_type=StringType(), required=True)
    ])
    new = make_schema([
        NestedField(field_id=1, name="name", field_type=StringType(), required=True)
    ])
    diff = SchemaDiff.from_schemas(current, new)
    assert len(diff.changed) == 1
    change = diff.changed[0]
    assert change.change == "renamed"
    assert change.previous_name == "first_name"
    assert change.name == "name"


def test_type_changed_diff():
    """Should detect a type change as 'type_changed'."""
    current = make_schema([
        NestedField(field_id=1, name="age", field_type=IntegerType(), required=True)
    ])
    new = make_schema([
        NestedField(field_id=1, name="age", field_type=StringType(), required=True)
    ])
    diff = SchemaDiff.from_schemas(current, new)
    assert len(diff.changed) == 1
    assert diff.changed[0].change == "type_changed"


def test_doc_changed_diff():
    """Should detect doc string change as 'doc_changed'."""
    current = make_schema([
        NestedField(field_id=1, name="age", field_type=IntegerType(), required=True, doc="years")
    ])
    new = make_schema([
        NestedField(field_id=1, name="age", field_type=IntegerType(), required=True, doc="user age")
    ])
    diff = SchemaDiff.from_schemas(current, new)
    assert len(diff.changed) == 1
    assert diff.changed[0].change == "doc_changed"


def test_nested_struct_diff():
    """Should recurse into struct fields and detect nested changes."""
    current = make_schema([
        NestedField(1, "user", StructType(
            NestedField(2, "name", StringType(), required=True)
        ), required=True)
    ])
    new = make_schema([
        NestedField(1, "user", StructType(
            NestedField(2, "name", StringType(), required=True),
            NestedField(3, "email", StringType(), required=True)
        ), required=True)
    ])
    diff = SchemaDiff.from_schemas(current, new)
    assert len(diff.added) == 1
    assert diff.added[0].name == "user.email"


def test_to_evolution_operations_all_cases():
    """Should convert added, removed, renamed, type/doc changed, and moved fields into correct evolution operations."""
    from pyiceberg.types import IntegerType, StringType
    from iceberg_evolve.diff import FieldChange, SchemaDiff
    from iceberg_evolve.migrate import (
        AddColumn,
        DropColumn,
        RenameColumn,
        UpdateColumn,
        MoveColumn
    )

    diff = SchemaDiff(
        added=[FieldChange(name="new_field", change="added", new_type=StringType(), doc="new")],
        removed=[FieldChange(name="old_field", change="removed", current_type=IntegerType(), doc="old")],
        changed=[
            FieldChange(name="full_name", previous_name="name", change="renamed"),
            FieldChange(name="age", change="type_changed", current_type=IntegerType(), new_type=StringType(), doc="converted"),
            FieldChange(name="email", change="doc_changed", current_type=StringType(), new_type=StringType(), doc="updated doc"),
            FieldChange(name="address", change="moved", position="before", relative_to="city")
        ]
    )
    ops = diff.to_evolution_operations()

    assert any(isinstance(op, AddColumn) and op.name == "new_field" for op in ops)
    assert any(isinstance(op, DropColumn) and op.name == "old_field" for op in ops)
    assert any(isinstance(op, RenameColumn) and op.name == "name" and op.target == "full_name" for op in ops)
    assert any(isinstance(op, UpdateColumn) and op.name == "age" and op.doc == "converted" for op in ops)
    assert any(isinstance(op, UpdateColumn) and op.name == "email" and op.doc == "updated doc" for op in ops)
    assert any(isinstance(op, MoveColumn) and op.name == "address" and op.target == "city" and op.position == "before" for op in ops)


def test_str_output_contains_all_sections():
    """Should render all diff sections in __str__."""
    current = make_schema([
        NestedField(1, "foo", IntegerType(), required=True)
    ])
    new = make_schema([
        NestedField(1, "foo", StringType(), required=True),
        NestedField(2, "bar", StringType(), required=True)
    ])
    diff = SchemaDiff.from_schemas(current, new)
    text = str(diff)
    assert "ADDED:" in text
    assert "CHANGED:" in text


def test_fieldchange_pretty_for_added():
    """Test that FieldChange.pretty() returns expected string for 'added' fields."""
    change = FieldChange(
        name="email",
        change="added",
        new_type=StringType()
    )
    pretty_output = change.pretty()
    assert pretty_output == "email: string"


def test_fieldchange_pretty_for_removed():
    """Test that FieldChange.pretty() returns expected string for 'removed' fields."""
    change = FieldChange(name="email", change="removed")
    assert change.pretty() == "email"


def test_fieldchange_pretty_for_type_changed():
    """Test that FieldChange.pretty() returns expected string for 'type_changed' fields."""
    change = FieldChange(
        name="age",
        change="type_changed",
        current_type=IntegerType(),
        new_type=StringType()
    )
    expected = "age:\n  from: int\n    to: string"
    assert change.pretty() == expected


def test_fieldchange_pretty_for_doc_changed():
    """Test that FieldChange.pretty() returns expected string for 'doc_changed' fields."""
    change = FieldChange(
        name="age",
        change="doc_changed"
    )
    assert change.pretty() == "age: doc changed"


def test_fieldchange_pretty_for_renamed():
    """Test that FieldChange.pretty() returns expected string for 'renamed' fields."""
    change = FieldChange(
        name="full_name",
        change="renamed",
        previous_name="name"
    )
    assert change.pretty() == "name renamed to full_name"


def test_fieldchange_pretty_for_moved():
    """Test that FieldChange.pretty() returns expected string for 'moved' fields."""
    change = FieldChange(
        name="address",
        change="moved",
        position="after",
        relative_to="email"
    )
    assert change.pretty() == "address moved after email"


def test_fieldchange_pretty_for_unknown_change_type():
    """Test that FieldChange.pretty() returns fallback string for unknown change types."""
    change = FieldChange(name="unknown", change="foobar")
    assert str(change) == change.pretty()


def test_display_delegates_to_renderer(monkeypatch):
    """Should delegate rendering to SchemaDiffRenderer.display()."""
    mock_called = {}

    class MockRenderer:
        def __init__(self, diff, console):
            mock_called["diff"] = diff
            mock_called["console"] = console

        def display(self):
            mock_called["called"] = True

    monkeypatch.setitem(
        sys.modules,
        "iceberg_evolve.renderer",
        type("mock_mod", (), {"SchemaDiffRenderer": MockRenderer})
    )

    diff = SchemaDiff(added=[], removed=[], changed=[])
    diff.display(console="dummy-console")

    assert mock_called.get("called") is True
    assert mock_called.get("diff") is diff
    assert mock_called.get("console") == "dummy-console"


def test_from_schemas_invalid_types():
    """Test that SchemaDiff.from_schemas raises ValueError for invalid input types."""
    from iceberg_evolve.schema import Schema as EvolveSchema

    # Prepare valid schema instances
    iceberg_schema = Schema(
        NestedField(field_id=1, name="id", field_type=StringType(), required=True)
    )
    evolve_schema = EvolveSchema(iceberg_schema)

    # Invalid current type
    with pytest.raises(ValueError, match="Both current and new must be instances"):
        SchemaDiff.from_schemas(current="not_a_schema", new=evolve_schema)

    # Invalid new type
    with pytest.raises(ValueError, match="Both current and new must be instances"):
        SchemaDiff.from_schemas(current=evolve_schema, new=123)


# --- Tests for minimal_moves logic in SchemaDiff.from_schemas ---
def test_move_detection_for_swapped_columns():
    """Test that swapping two columns flags both as moved via minimal_moves logic."""
    # Schema with three fields: a, b, c
    current = make_schema([
        NestedField(1, "a", StringType(), required=True),
        NestedField(2, "b", StringType(), required=True),
        NestedField(3, "c", StringType(), required=True),
    ])
    # New schema swaps b and c
    new = make_schema([
        NestedField(1, "a", StringType(), required=True),
        NestedField(3, "c", StringType(), required=True),
        NestedField(2, "b", StringType(), required=True),
    ])
    diff = SchemaDiff.from_schemas(current, new)
    moved_names = sorted(fc.name for fc in diff.changed if fc.change == "moved")
    # minimal_moves flags both b and c as moved
    assert moved_names == ["c"]


def test_no_move_for_identical_order():
    """Test that no moved fields are reported when the order is identical."""
    fields = [
        NestedField(1, "a", StringType(), required=True),
        NestedField(2, "b", StringType(), required=True),
        NestedField(3, "c", StringType(), required=True),
    ]
    current = make_schema(fields)
    new = make_schema(fields)
    diff = SchemaDiff.from_schemas(current, new)
    # Ensure no 'moved' changes
    assert all(fc.change != "moved" for fc in diff.changed)


def test_swap_two_fields_reports_both_moved():
    """Test that swapping two fields in two-element schema flags both as moved."""
    current = make_schema([
        NestedField(1, "x", StringType(), required=True),
        NestedField(2, "y", StringType(), required=True),
    ])
    new = make_schema([
        NestedField(2, "y", StringType(), required=True),
        NestedField(1, "x", StringType(), required=True),
    ])
    diff = SchemaDiff.from_schemas(current, new)
    moved = [fc.name for fc in diff.changed if fc.change == "moved"]
    assert set(moved) == {"y"}


def test_union_by_name_identical_schemas_no_diff():
    """Union by name on identical schemas yields no added/changed/removed."""
    # Same name and type, different IDs → no diff
    f1 = NestedField(1, "a", StringType(), required=True)
    f2 = NestedField(2, "a", StringType(), required=True)
    cur = make_schema([f1])
    new = make_schema([f2])
    diff = SchemaDiff.union_by_name(cur, new)
    assert diff.added == []
    assert diff.removed == []
    assert diff.changed == []


def test_union_by_name_detects_added_field():
    """Fields present only in the new schema show up as 'added'."""
    cur = make_schema([NestedField(1, "a", StringType(), required=True)])
    new = make_schema([
        NestedField(1, "a", StringType(), required=True),
        NestedField(2, "b", IntegerType(), required=False)
    ])
    diff = SchemaDiff.union_by_name(cur, new)
    assert len(diff.added) == 1
    added = diff.added[0]
    assert added.name == "b"
    assert isinstance(added.new_type, IntegerType)
    assert diff.removed == []
    assert diff.changed == []


def test_union_by_name_detects_type_changed():
    """Fields with same name but different types show up under 'changed'."""
    cur = make_schema([NestedField(1, "a", StringType(), required=True)])
    new = make_schema([NestedField(1, "a", IntegerType(), required=True)])
    diff = SchemaDiff.union_by_name(cur, new)
    assert diff.added == []
    assert diff.removed == []
    assert len(diff.changed) == 1
    change = diff.changed[0]
    assert change.name == "a"
    assert isinstance(change.current_type, StringType)
    assert isinstance(change.new_type, IntegerType)


def test_union_by_name_never_reports_removed():
    """Union by name should never report removed fields."""
    cur = make_schema([
        NestedField(1, "a", StringType(), required=True),
        NestedField(2, "b", IntegerType(), required=False)
    ])
    new = make_schema([NestedField(1, "a", StringType(), required=True)])
    diff = SchemaDiff.union_by_name(cur, new)
    assert diff.added == []
    assert diff.removed == []  # never removes
    # and 'b' should not appear in changed either
    assert all(c.name != "b" for c in diff.changed)


def test_union_by_name_detects_list_type_change():
    """Union by name should detect nested ListType element-type changes."""
    cur = make_schema([
        NestedField(
            1,
            "c",
            ListType(element_id=3, element_type=IntegerType(), element_required=True),
            required=False
        )
    ])
    new = make_schema([
        NestedField(
            1,
            "c",
            ListType(element_id=3, element_type=StringType(), element_required=True),
            required=False
        )
    ])
    diff = SchemaDiff.union_by_name(cur, new)
    assert len(diff.changed) == 1
    change = diff.changed[0]
    assert change.name == "c"
    assert isinstance(change.current_type, ListType)
    assert isinstance(change.new_type, ListType)
