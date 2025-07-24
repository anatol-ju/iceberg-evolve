def test_evolution_operations_renderer_filters_nested_and_inserts_blank_lines():
    """Test that EvolutionOperationsRenderer filters nested ops and inserts blank lines between different op types."""
    from iceberg_evolve.renderer import EvolutionOperationsRenderer
    from iceberg_evolve.evolution_operation import AddColumn, DropColumn
    from pyiceberg.types import StringType

    printed = []
    class DummyConsole:
        def print(self, obj=None):
            printed.append(obj)

    # Include a nested op 'x.y' which should be filtered out
    ops = [
        AddColumn(name="x", new_type=StringType()),
        AddColumn(name="x.y", new_type=StringType()),
        DropColumn(name="y")
    ]
    renderer = EvolutionOperationsRenderer(ops, console=DummyConsole())
    renderer.display()

    # The nested 'x.y' should be filtered; only 'x' and 'y' printed
    # 'x' (AddColumn) then blank line then 'y' (DropColumn)
    assert len(printed) == 3
    # First printed object is the Tree for AddColumn 'x'
    first = printed[0]
    from rich.tree import Tree
    assert isinstance(first, Tree)
    assert "+ x" in first.label
    # Second printed object is a blank line (None)
    assert printed[1] is None
    # Third printed object is the Tree for DropColumn 'y'
    third = printed[2]
    assert isinstance(third, Tree)
    assert "- y" in third.label

def test_evolution_operations_renderer_displays_updatecolumn_and_nested_diff(monkeypatch):
    """Test that EvolutionOperationsRenderer.display prints UpdateColumn and then nested SchemaDiff."""
    from iceberg_evolve.renderer import EvolutionOperationsRenderer
    from iceberg_evolve.evolution_operation import UpdateColumn
    from iceberg_evolve.diff import SchemaDiff, FieldChange
    from pyiceberg.types import StringType, IntegerType

    printed = []
    class DummyConsole:
        def print(self, obj=None):
            printed.append(obj)

    # Create an UpdateColumn with nested_changes
    op = UpdateColumn(name="col", current_type=StringType(), new_type=IntegerType())
    op.nested_changes = [
        FieldChange(name="col.sub", change="removed")
    ]

    renderer = EvolutionOperationsRenderer([op], console=DummyConsole())
    renderer.display()

    from rich.tree import Tree
    from rich.console import Group

    # First printed object is the Tree from op.pretty()
    assert isinstance(printed[0], Tree)
    assert "~ col" in printed[0].label

    # Second printed object is the nested diff Group
    assert any(isinstance(obj, Group) for obj in printed[1:])
