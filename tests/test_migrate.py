import sys
from unittest.mock import MagicMock

import pytest
from pyiceberg.types import IntegerType, LongType, StringType, StructType
from rich.tree import Tree

from iceberg_evolve.exceptions import UnsupportedSchemaEvolutionWarning
from iceberg_evolve.migrate import (
    AddColumn,
    BaseEvolutionOperation,
    DropColumn,
    MoveColumn,
    RenameColumn,
    UnionSchema,
    UpdateColumn
)


@pytest.fixture
def mock_update_schema():
    mock = MagicMock()
    return mock

class DummyOp(BaseEvolutionOperation):
    def to_serializable_dict(self):
        return {"operation": "dummy", "name": self.name}

    def pretty(self, use_color=False):
        from rich.tree import Tree
        return Tree("dummy")

    def apply(self, update):
        update.call_dummy(self.name)

def test_base_evolution_operation_to_serializable_dict_not_implemented():
    """Test that BaseEvolutionOperation.to_serializable_dict raises NotImplementedError."""
    base_op = BaseEvolutionOperation(name="col")
    with pytest.raises(NotImplementedError):
        base_op.to_serializable_dict()

def test_base_evolution_operation_pretty_not_implemented():
    """Test that BaseEvolutionOperation.pretty raises NotImplementedError."""
    base_op = BaseEvolutionOperation(name="col")
    with pytest.raises(NotImplementedError):
        base_op.pretty()

def test_base_evolution_operation_apply_not_implemented():
    """Test that BaseEvolutionOperation.apply raises NotImplementedError."""
    base_op = BaseEvolutionOperation(name="col")
    with pytest.raises(NotImplementedError):
        base_op.apply(None)

def test_base_evolution_operation_is_breaking_false():
    """Test BaseEvolutionOperation default is_breaking returns False."""
    base_op = BaseEvolutionOperation(name="col")
    assert base_op.is_breaking() is False

def test_base_evolution_operation_display(monkeypatch):
    """Test BaseEvolutionOperation.display calls EvolutionOperationsRenderer with self."""
    dummy_op = DummyOp(name="col1")
    mock_renderer = MagicMock()
    monkeypatch.setitem(
        sys.modules,
        "iceberg_evolve.renderer",
        type("mock_renderer_module", (), {"EvolutionOperationsRenderer": mock_renderer})
    )

    dummy_op.display()
    mock_renderer.assert_called_once()
    args, _ = mock_renderer.call_args
    assert args[0] == [dummy_op]

def test_add_column_to_serializable_dict_with_doc():
    """Test AddColumn.to_serializable_dict includes doc if provided."""
    op = AddColumn(name="new_col", new_type=StringType(), doc="sample doc")
    result = op.to_serializable_dict()
    assert result == {
        "operation": "add_column",
        "name": "new_col",
        "to": "string",
        "doc": "sample doc",
    }

def test_add_column_to_serializable_dict_without_doc():
    """Test AddColumn.to_serializable_dict omits doc if not provided."""
    op = AddColumn(name="new_col", new_type=StringType())
    result = op.to_serializable_dict()
    assert result == {
        "operation": "add_column",
        "name": "new_col",
        "to": "string",
    }

def test_add_column_pretty_returns_tree():
    """Test AddColumn.pretty returns a Tree instance."""
    op = AddColumn(name="new_col", new_type=StringType())
    tree = op.pretty()
    assert isinstance(tree, Tree)
    assert any("new_col" in child.label for child in tree.children)

def test_add_column_apply_flat_column(mock_update_schema):
    """Test AddColumn.apply calls update.add_column with flat name."""
    op = AddColumn(name="col1", new_type=StringType())
    op.apply(mock_update_schema)
    mock_update_schema.add_column.assert_called_once_with("col1", StringType(), doc=None)

def test_add_column_apply_nested_column(mock_update_schema):
    """Test AddColumn.apply splits dotted name into path tuple."""
    op = AddColumn(name="parent.child", new_type=StringType(), doc="nested doc")
    op.apply(mock_update_schema)
    mock_update_schema.add_column.assert_called_once_with(("parent", "child"), StringType(), doc="nested doc")

def test_drop_column_to_serializable_dict():
    """Test DropColumn.to_serializable_dict produces correct output."""
    op = DropColumn(name="old_col")
    result = op.to_serializable_dict()
    assert result == {
        "operation": "drop_column",
        "name": "old_col",
    }

def test_drop_column_pretty_returns_tree():
    """Test DropColumn.pretty returns a Tree with correct label and name."""
    op = DropColumn(name="old_col")
    tree = op.pretty()
    assert isinstance(tree, Tree)
    assert any("old_col" in child.label for child in tree.children)

def test_drop_column_apply_flat_column(mock_update_schema):
    """Test DropColumn.apply calls delete_column with flat name."""
    op = DropColumn(name="col1")
    op.apply(mock_update_schema)
    mock_update_schema.delete_column.assert_called_once_with("col1")

def test_drop_column_apply_nested_column(mock_update_schema):
    """Test DropColumn.apply splits dotted name into path tuple."""
    op = DropColumn(name="parent.child")
    op.apply(mock_update_schema)
    mock_update_schema.delete_column.assert_called_once_with(("parent", "child"))

def test_drop_column_is_breaking():
    """Test DropColumn.is_breaking always returns True."""
    op = DropColumn(name="any_col")
    assert op.is_breaking() is True

def test_update_column_to_serializable_dict_with_doc():
    """Test UpdateColumn.to_serializable_dict includes doc if provided."""
    op = UpdateColumn(name="col1", current_type=StringType(), new_type=IntegerType(), doc="changed type")
    result = op.to_serializable_dict()
    assert result == {
        "operation": "update_column_type",
        "name": "col1",
        "from": "string",
        "to": "int",
        "doc": "changed type",
    }

def test_update_column_to_serializable_dict_without_doc():
    """Test UpdateColumn.to_serializable_dict omits doc if not provided."""
    op = UpdateColumn(name="col1", current_type=StringType(), new_type=IntegerType())
    result = op.to_serializable_dict()
    assert result == {
        "operation": "update_column_type",
        "name": "col1",
        "from": "string",
        "to": "int",
    }

def test_update_column_pretty_simple_type():
    """Test UpdateColumn.pretty correctly renders a basic type change."""
    op = UpdateColumn(name="col1", current_type=StringType(), new_type=IntegerType())
    tree = op.pretty()
    assert isinstance(tree, Tree)
    assert any("from: string" in child.label for child in tree.children[0].children)
    assert any("to: int" in child.label for child in tree.children[0].children)

def test_update_column_pretty_struct_type():
    """Test UpdateColumn.pretty handles struct types with type_to_tree rendering."""
    struct = StructType()
    op = UpdateColumn(name="col1", current_type=struct, new_type=struct)
    tree = op.pretty()
    assert isinstance(tree, Tree)
    assert "col1" in tree.children[0].label

def test_update_column_apply_flat_column(mock_update_schema):
    """Test UpdateColumn.apply with flat column name calls update_column correctly."""
    op = UpdateColumn(name="col1", current_type=StringType(), new_type=IntegerType())
    op.apply(mock_update_schema)
    mock_update_schema.update_column.assert_called_once_with("col1", field_type=IntegerType())

def test_update_column_apply_nested_column(mock_update_schema):
    """Test UpdateColumn.apply with nested column path."""
    op = UpdateColumn(name="parent.child", current_type=StringType(), new_type=IntegerType())
    op.apply(mock_update_schema)
    mock_update_schema.update_column.assert_called_once_with(("parent", "child"), field_type=IntegerType())

def test_update_column_is_breaking_true():
    """Test UpdateColumn.is_breaking returns True when narrowing the type."""
    op = UpdateColumn(name="col1", current_type=LongType(), new_type=IntegerType())
    assert op.is_breaking() is True

def test_update_column_is_breaking_false():
    """Test UpdateColumn.is_breaking returns False when not narrowing the type."""
    op = UpdateColumn(name="col1", current_type=StringType(), new_type=StringType())
    assert op.is_breaking() is False

def test_rename_column_to_serializable_dict():
    """Test RenameColumn.to_serializable_dict returns correct structure."""
    op = RenameColumn(name="old_name", target="new_name")
    result = op.to_serializable_dict()
    assert result == {
        "operation": "rename_column",
        "name": "old_name",
        "to": "new_name",
    }

def test_rename_column_pretty_returns_tree():
    """Test RenameColumn.pretty returns a Tree with old and new names."""
    op = RenameColumn(name="old_name", target="new_name")
    tree = op.pretty()
    assert isinstance(tree, Tree)
    assert "old_name" in tree.children[0].label
    assert any("to: new_name" in child.label for child in tree.children[0].children)

def test_rename_column_apply_flat_column(mock_update_schema):
    """Test RenameColumn.apply calls rename_column with flat name."""
    op = RenameColumn(name="col1", target="renamed_col")
    op.apply(mock_update_schema)
    mock_update_schema.rename_column.assert_called_once_with("col1", "renamed_col")

def test_rename_column_apply_nested_column(mock_update_schema):
    """Test RenameColumn.apply with nested column path."""
    op = RenameColumn(name="parent.child", target="renamed_child")
    op.apply(mock_update_schema)
    mock_update_schema.rename_column.assert_called_once_with(("parent", "child"), "renamed_child")


def test_move_column_to_serializable_dict():
    """Test MoveColumn.to_serializable_dict returns correct structure."""
    op = MoveColumn(name="col1", target="col2", position="before")
    result = op.to_serializable_dict()
    assert result == {
        "operation": "move_column",
        "name": "col1",
        "position": "before",
        "target": "col2",
    }

def test_move_column_pretty_returns_tree():
    """Test MoveColumn.pretty returns a Tree with correct formatting."""
    op = MoveColumn(name="col1", target="col2", position="after")
    tree = op.pretty()
    assert isinstance(tree, Tree)
    assert "col1" in tree.children[0].label
    assert any("from: after" in child.label for child in tree.children[0].children)
    assert any("of: col2" in child.label for child in tree.children[0].children)

def test_move_column_apply_first(mock_update_schema):
    """Test MoveColumn.apply calls move_first for position 'first'."""
    op = MoveColumn(name="col1", target="", position="first")
    op.apply(mock_update_schema)
    mock_update_schema.move_first.assert_called_once_with("col1")

def test_move_column_apply_before(mock_update_schema):
    """Test MoveColumn.apply calls move_before for position 'before'."""
    op = MoveColumn(name="col1", target="col2", position="before")
    op.apply(mock_update_schema)
    mock_update_schema.move_before.assert_called_once_with("col1", "col2")

def test_move_column_apply_after(mock_update_schema):
    """Test MoveColumn.apply calls move_after for position 'after'."""
    op = MoveColumn(name="col1", target="col2", position="after")
    op.apply(mock_update_schema)
    mock_update_schema.move_after.assert_called_once_with("col1", "col2")

def test_move_column_apply_nested_first(mock_update_schema):
    """Test MoveColumn.apply handles nested column path with 'first'."""
    op = MoveColumn(name="parent.child", target="", position="first")
    op.apply(mock_update_schema)
    mock_update_schema.move_first.assert_called_once_with(("parent", "child"))

def test_union_schema_to_serializable_dict():
    """Test UnionSchema.to_serializable_dict returns correct structure."""
    op = UnionSchema(name="merged", new_type=StructType())
    result = op.to_serializable_dict()
    assert result == {
        "operation": "union_schema",
        "with": "struct<>"
    }

def test_union_schema_pretty_returns_tree():
    """Test UnionSchema.pretty returns a Tree with name and type."""
    op = UnionSchema(name="merged", new_type=StructType())
    tree = op.pretty()
    assert isinstance(tree, Tree)
    assert "merged" in tree.children[0].label
    assert any("with type: struct<>" in child.label for child in tree.children[0].children)


def test_union_schema_apply_warns_when_unsupported(mock_update_schema):
    """UnionSchema.apply should warn and not call union_by_name when unsupported."""
    import pytest
    from iceberg_evolve.exceptions import UnsupportedSchemaEvolutionWarning
    from pyiceberg.types import StructType
    from iceberg_evolve.migrate import UnionSchema

    new_type = StructType()
    op = UnionSchema(name="merged", new_type=new_type)

    with pytest.warns(UnsupportedSchemaEvolutionWarning,
                      match="Skipping unsupported operation: UnionSchema on 'merged'"):
        op.apply(mock_update_schema)

    # Since it’s unsupported, it must not invoke the catalog call
    assert not mock_update_schema.union_by_name.called


# Additional tests for .pretty(use_color=True) for all subclasses of BaseEvolutionOperation
def test_add_column_pretty_with_color():
    """Test AddColumn.pretty with use_color=True."""
    op = AddColumn(name="new_col", new_type=StringType())
    tree = op.pretty(use_color=True)
    assert isinstance(tree, Tree)
    assert any("new_col" in child.label for child in tree.children)

def test_drop_column_pretty_with_color():
    """Test DropColumn.pretty with use_color=True."""
    op = DropColumn(name="old_col")
    tree = op.pretty(use_color=True)
    assert isinstance(tree, Tree)
    assert any("old_col" in child.label for child in tree.children)

def test_update_column_pretty_with_color():
    """Test UpdateColumn.pretty with use_color=True."""
    op = UpdateColumn(name="col1", current_type=StringType(), new_type=IntegerType())
    tree = op.pretty(use_color=True)
    assert isinstance(tree, Tree)
    assert "col1" in tree.children[0].label

def test_rename_column_pretty_with_color():
    """Test RenameColumn.pretty with use_color=True."""
    op = RenameColumn(name="old_name", target="new_name")
    tree = op.pretty(use_color=True)
    assert isinstance(tree, Tree)
    assert "old_name" in tree.children[0].label

def test_move_column_pretty_with_color():
    """Test MoveColumn.pretty with use_color=True."""
    op = MoveColumn(name="col1", target="col2", position="after")
    tree = op.pretty(use_color=True)
    assert isinstance(tree, Tree)
    assert "col1" in tree.children[0].label

def test_union_schema_pretty_with_color():
    """Test UnionSchema.pretty with use_color=True."""
    op = UnionSchema(name="merged", new_type=StructType())
    tree = op.pretty(use_color=True)
    assert isinstance(tree, Tree)
    assert "merged" in tree.children[0].label


def test_add_column_pretty_unsupported():
    from iceberg_evolve.migrate import AddColumn
    from pyiceberg.types import StringType
    op = AddColumn("foo", StringType())
    op.is_supported = False
    tree = op.pretty(use_color=False)
    assert "⚠️" in tree.label      # covers line 73


def test_add_column_apply_unsupported(mock_update_schema):
    from iceberg_evolve.migrate import AddColumn
    from pyiceberg.types import StringType
    op = AddColumn("foo", StringType())
    op.is_supported = False
    with pytest.warns(UnsupportedSchemaEvolutionWarning):
        op.apply(mock_update_schema)  # covers lines 86–90
    mock_update_schema.add_column.assert_not_called()


def test_drop_column_pretty_unsupported():
    from iceberg_evolve.migrate import DropColumn
    op = DropColumn("bar")
    op.is_supported = False
    tree = op.pretty(use_color=True)
    assert "⚠️" in tree.label    # covers line 120


def test_drop_column_apply_unsupported(mock_update_schema):
    from iceberg_evolve.migrate import DropColumn
    op = DropColumn("bar")
    op.is_supported = False
    with pytest.warns(UnsupportedSchemaEvolutionWarning):
        op.apply(mock_update_schema)  # covers lines 133–137
    mock_update_schema.delete_column.assert_not_called()


def test_update_column_pretty_unsupported():
    from iceberg_evolve.migrate import UpdateColumn
    from pyiceberg.types import StringType, StructType
    op = UpdateColumn("baz", current_type=StringType(), new_type=StructType())
    # __post_init__ sets is_supported=False for non-primitive new_type
    tree = op.pretty(use_color=False)
    assert "⚠️" in tree.label    # covers the Unsupported suffix in pretty()


def test_update_column_apply_unsupported(mock_update_schema):
    from iceberg_evolve.migrate import UpdateColumn
    from pyiceberg.types import StringType, StructType
    op = UpdateColumn("baz", current_type=StringType(), new_type=StructType())
    with pytest.warns(UnsupportedSchemaEvolutionWarning):
        op.apply(mock_update_schema)  # covers lines 216–224
    mock_update_schema.update_column.assert_not_called()


def test_rename_column_pretty_unsupported():
    from iceberg_evolve.migrate import RenameColumn
    op = RenameColumn("old", target="new")
    op.is_supported = False
    tree = op.pretty(use_color=True)
    assert "⚠️" in tree.label    # covers line 267


def test_rename_column_apply_unsupported(mock_update_schema):
    from iceberg_evolve.migrate import RenameColumn
    op = RenameColumn("old", target="new")
    op.is_supported = False
    with pytest.warns(UnsupportedSchemaEvolutionWarning):
        op.apply(mock_update_schema)  # covers lines 281–285
    mock_update_schema.rename_column.assert_not_called()


def test_move_column_pretty_unsupported():
    from iceberg_evolve.migrate import MoveColumn
    op = MoveColumn("m", target="t", position="first")
    op.is_supported = False
    tree = op.pretty(use_color=False)
    assert "⚠️" in tree.label    # covers line 322


def test_move_column_apply_unsupported(mock_update_schema):
    from iceberg_evolve.migrate import MoveColumn
    op = MoveColumn("m", target="t", position="after")
    op.is_supported = False
    with pytest.warns(UnsupportedSchemaEvolutionWarning):
        op.apply(mock_update_schema)  # covers lines 337–341
    mock_update_schema.move_after.assert_not_called()


def test_union_schema_apply_supported(mock_update_schema):
    from iceberg_evolve.migrate import UnionSchema
    from pyiceberg.types import StructType
    op = UnionSchema("union_me", new_type=StructType())
    # override to True so we hit the final line 405
    op.is_supported = True
    op.apply(mock_update_schema)
    mock_update_schema.union_by_name.assert_called_once_with(op.new_type)
