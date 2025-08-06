import pytest
from rich.tree import Tree

from iceberg_evolve.diff import FieldChange, SchemaDiff
from iceberg_evolve.migrate import AddColumn, DropColumn, UnionSchema, UpdateColumn
from iceberg_evolve.renderer import SchemaDiffRenderer
from pyiceberg.types import IntegerType, ListType, NestedField, StringType, StructType


def test_walk_and_color_struct_highlighting():
    """Test that _walk_and_color highlights removed, added, and type_changed for StructType fields."""
    # Define a struct with three fields
    struct = StructType(
        NestedField(field_id=1, name="f1", field_type=StringType(), required=True),
        NestedField(field_id=2, name="f2", field_type=IntegerType(), required=False),
        NestedField(field_id=3, name="f3", field_type=StringType(), required=False),
    )
    # Create a diff marking f1 removed, f2 added, f3 type_changed
    removed = [FieldChange(name="base.f1", change="removed")]
    added = [FieldChange(name="base.f2", change="added", new_type=IntegerType())]
    changed = [FieldChange(name="base.f3", change="type_changed", current_type=StringType(), new_type=StringType())]
    diff = SchemaDiff(added=added, removed=removed, changed=changed)
    renderer = SchemaDiffRenderer(diff, console=None)

    # Side = 'from': f1 should be red, f2 & f3 unstyled
    root_from = Tree("root")
    renderer._walk_and_color(root_from, struct, side="from", base="base")
    labels_from = [child.label for child in root_from.children]
    assert "[red]f1: string required[/red]" in labels_from
    assert any("f2: int" == lbl for lbl in labels_from)
    assert any("f3: string" == lbl for lbl in labels_from)

    # Side = 'to': f2 green, f3 yellow, f1 unstyled
    root_to = Tree("root")
    renderer._walk_and_color(root_to, struct, side="to", base="base")
    labels_to = [child.label for child in root_to.children]
    assert "[green]f2: int[/green]" in labels_to
    assert "[yellow]f3: string[/yellow]" in labels_to
    assert any("f1: string required" == lbl for lbl in labels_to)


def test_walk_and_color_list_primitive_inline():
    """Test that _walk_and_color inlines ListType of primitive types."""
    lt = ListType(element_id=5, element_type=IntegerType(), element_required=True)
    diff = SchemaDiff(added=[], removed=[], changed=[])
    renderer = SchemaDiffRenderer(diff, console=None)

    root = Tree("root")
    renderer._walk_and_color(root, lt, side="to", base="base")
    # Should add a single child with clean_type_str of the list
    assert any(child.label == "list<int>" for child in root.children)


def test_walk_and_color_struct_recursion():
    """Test _walk_and_color recurses into nested StructType fields correctly."""
    # Define a nested struct: parent -> child
    nested = StructType(
        NestedField(field_id=2, name="child", field_type=StringType(), required=False)
    )
    struct = StructType(
        NestedField(field_id=1, name="parent", field_type=nested, required=True)
    )
    diff = SchemaDiff(added=[], removed=[], changed=[])
    renderer = SchemaDiffRenderer(diff, console=None)

    root = Tree("root")
    renderer._walk_and_color(root, struct, side="to", base="")
    # Expect one top-level child for 'parent'
    assert len(root.children) == 1
    parent_node = root.children[0]
    # Label for struct field
    assert parent_node.label == "parent: struct required"
    # The struct node should have a child 'child'
    assert len(parent_node.children) == 1
    assert parent_node.children[0].label == "child: string"


def test_walk_and_color_list_of_struct_branch():
    """Test _walk_and_color handles a ListType whose element_type is StructType."""
    # Define struct element
    element_struct = StructType(
        NestedField(field_id=2, name="sub", field_type=StringType(), required=False)
    )
    # Wrap in a list
    struct = StructType(
        NestedField(
            field_id=1,
            name="ls",
            field_type=ListType(element_id=3, element_type=element_struct, element_required=True),
            required=False
        )
    )
    diff = SchemaDiff(added=[], removed=[], changed=[])
    renderer = SchemaDiffRenderer(diff, console=None)

    root = Tree("root")
    renderer._walk_and_color(root, struct, side="to", base="")
    # Expect one child for the list
    assert len(root.children) == 1
    list_node = root.children[0]
    # Label for list<struct>
    assert list_node.label == "ls: list<struct>"
    # List node should recurse into element_struct, showing 'sub'
    assert len(list_node.children) == 1
    assert list_node.children[0].label == "sub: string"


def test_walk_and_color_list_of_primitives_branch():
    """Test _walk_and_color handles a ListType of primitive element types inline."""
    # Use a list of integers inside a struct to hit primitive-list branch
    struct = StructType(
        NestedField(
            field_id=1,
            name="lp",
            field_type=ListType(element_id=4, element_type=IntegerType(), element_required=False),
            required=False
        )
    )
    diff = SchemaDiff(added=[], removed=[], changed=[])
    renderer = SchemaDiffRenderer(diff, console=None)

    root = Tree("root")
    renderer._walk_and_color(root, struct, side="to", base="")
    # Inline list: single child with label
    assert any(child.label == "lp: list<int>" for child in root.children)


def test_walk_and_color_primitive_branch():
    """Test _walk_and_color handles primitive fields directly."""
    struct = StructType(
        NestedField(field_id=1, name="p", field_type=StringType(), required=False)
    )
    diff = SchemaDiff(added=[], removed=[], changed=[])
    renderer = SchemaDiffRenderer(diff, console=None)

    root = Tree("root")
    renderer._walk_and_color(root, struct, side="to", base="")
    # Primitive field: single child 'p: string'
    assert root.children[0].label == "p: string"


@pytest.fixture
def renderer():
    """Provide a SchemaDiffRenderer with an empty diff."""
    diff = SchemaDiff(added=[], removed=[], changed=[])
    return SchemaDiffRenderer(diff, console=None)


def test_render_change_added_branch(renderer):
    """Test the 'added' branch of _render_change."""
    change = FieldChange(name="new_field", change="added", new_type=StringType())
    node = renderer._render_change("added", change)
    assert isinstance(node, Tree)
    assert "+ new_field" in node.label
    assert node.label.endswith(": string")


def test_render_change_removed_branch(renderer):
    """Test the 'removed' branch of _render_change."""
    change = FieldChange(name="old_field", change="removed")
    node = renderer._render_change("removed", change)
    assert isinstance(node, Tree)
    assert "- old_field" in node.label


def test_render_change_renamed_branch(renderer):
    """Test the 'renamed' branch of _render_change."""
    change = FieldChange(name="new_name", previous_name="old_name", change="renamed")
    node = renderer._render_change("changed", change)
    assert isinstance(node, Tree)
    assert "~ old_name" in node.label
    # Check child content
    child_labels = [child.label for child in node.children]
    assert any("renamed to:" in lbl and "new_name" in lbl for lbl in child_labels)


def test_render_change_type_changed_primitive(renderer):
    """Test 'type_changed' branch with primitive types."""
    change = FieldChange(
        name="field",
        change="type_changed",
        current_type=IntegerType(),
        new_type=StringType()
    )
    node = renderer._render_change("changed", change)
    assert isinstance(node, Tree)
    labels = [child.label for child in node.children]
    assert "from: int" in labels
    assert "to: string" in labels


def test_render_change_type_changed_struct(renderer):
    """Test 'type_changed' branch with a StructType."""
    struct = StructType(
        NestedField(field_id=1, name="f1", field_type=StringType(), required=True)
    )
    change = FieldChange(
        name="struct_field",
        change="type_changed",
        current_type=struct,
        new_type=struct
    )
    node = renderer._render_change("changed", change)
    assert isinstance(node, Tree)
    labels = [child.label for child in node.children]
    assert "from:" in labels
    assert "to:" in labels


def test_render_change_type_changed_list_of_struct(renderer):
    """Test 'type_changed' branch with a ListType of StructType."""
    inner = StructType(
        NestedField(field_id=2, name="sub", field_type=StringType(), required=False)
    )
    lt = ListType(element_id=1, element_type=inner, element_required=False)
    change = FieldChange(
        name="list_struct",
        change="type_changed",
        current_type=lt,
        new_type=lt
    )
    node = renderer._render_change("changed", change)
    assert isinstance(node, Tree)
    labels = [child.label for child in node.children]
    assert "from:" in labels
    assert "to:" in labels


def test_render_change_moved_branch(renderer):
    """Test the 'moved' branch of _render_change."""
    change = FieldChange(
        name="field",
        change="moved",
        position="after",
        relative_to="another"
    )
    node = renderer._render_change("changed", change)
    assert isinstance(node, Tree)
    # Label should start with ~ field
    assert "~ field" in node.label
    child_labels = [child.label for child in node.children]
    # Should include yellow formatting
    assert any("moved after:" in lbl and "another" in lbl for lbl in child_labels)


def test_render_change_doc_changed_branch(renderer):
    """Test the 'doc_changed' branch of _render_change."""
    change = FieldChange(
        name="field",
        change="doc_changed"
    )
    node = renderer._render_change("changed", change)
    assert isinstance(node, Tree)
    child_labels = [child.label for child in node.children]
    assert any("[yellow1]doc changed[/yellow1]" == lbl for lbl in child_labels)


def test_schema_diff_renderer_display_all_sections():
    """Test SchemaDiffRenderer.display prints all non-empty sections with correct headers."""
    from rich.console import Group
    from rich.text import Text
    from rich.tree import Tree

    # Prepare a diff with one change in each section
    added = [FieldChange(name="new", change="added", new_type=StringType())]
    removed = [FieldChange(name="old", change="removed")]
    changed = [FieldChange(name="renamed_new", previous_name="renamed_old", change="renamed")]
    diff = SchemaDiff(added=added, removed=removed, changed=changed)

    printed = []
    class DummyConsole:
        def print(self, obj):
            printed.append(obj)

    console = DummyConsole()
    renderer = SchemaDiffRenderer(diff, console=console)
    renderer.display()

    # Should have printed exactly one Group
    assert len(printed) == 1
    group = printed[0]
    assert isinstance(group, Group)

    # Expect sections: ADDED, REMOVED, CHANGED in order, separated by Text blank lines
    renderables = group.renderables
    # ADDED header
    assert isinstance(renderables[0], Tree)
    assert "ADDED" in renderables[0].label
    # blank Text
    assert isinstance(renderables[1], Text)
    # REMOVED header
    assert isinstance(renderables[2], Tree)
    assert "REMOVED" in renderables[2].label
    # blank Text
    assert isinstance(renderables[3], Text)
    # CHANGED header
    assert isinstance(renderables[4], Tree)
    assert "CHANGED" in renderables[4].label


def test_display_skips_empty_sections():
    """Test SchemaDiffRenderer.display skips sections with no changes."""
    from rich.console import Group

    # Only 'removed' section has a change
    diff = SchemaDiff(
        added=[],
        removed=[],
        changed=[]
    )

    printed = []
    class DummyConsole:
        def print(self, obj):
            printed.append(obj)

    renderer = SchemaDiffRenderer(diff, console=DummyConsole())
    renderer.display()

    # Should have printed one Group object
    assert len(printed) == 1
    assert isinstance(printed[0], Group)


def test_evolution_operations_renderer_filters_nested_and_inserts_blank_lines():
    """Test that EvolutionOperationsRenderer filters nested ops and inserts blank lines between different op types."""
    from iceberg_evolve.renderer import EvolutionOperationsRenderer
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
    assert len(first.children) == 1
    assert "+ x" in first.children[0].label
    # Second printed object is a blank line (None)
    assert printed[1] is None
    # Third printed object is the Tree for DropColumn 'y'
    third = printed[2]
    assert isinstance(third, Tree)
    assert len(third.children) == 1
    assert "- y" in third.children[0].label

def test_evolution_operations_renderer_displays_updatecolumn_and_nested_diff(monkeypatch):
    """Test that EvolutionOperationsRenderer.display prints UpdateColumn and then nested SchemaDiff."""
    from iceberg_evolve.renderer import EvolutionOperationsRenderer

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

    from rich.console import Group
    from rich.tree import Tree

    # First printed object is the Tree from op.pretty()
    assert isinstance(printed[0], Tree)
    assert len(printed[0].children) == 1
    assert "~ col" in printed[0].children[0].label

    # Second printed object is the nested diff Group
    assert any(isinstance(obj, Group) for obj in printed[1:])


def test_evolution_operations_renderer_warns_on_unsupported():
    """Test EvolutionOperationsRenderer.display emits warning messages for unsupported ops."""
    from iceberg_evolve.renderer import EvolutionOperationsRenderer

    printed = []
    class DummyConsole:
        def print(self, obj=None):
            printed.append(obj)

    # UnionSchema is always unsupported (is_supported=False)
    op = UnionSchema(name="u", new_type=StructType())
    renderer = EvolutionOperationsRenderer([op], console=DummyConsole())
    renderer.display()

    # 1) First printed item is the operation’s Tree
    assert isinstance(printed[0], Tree)

    # 2) Next two prints are our warnings
    #    a) The ⚠️ “Warning:” line
    #    b) The “Consider adding...” suggestion line
    assert len(printed) >= 3
    warning_line    = printed[1]
    suggestion_line = printed[2]

    assert isinstance(warning_line, str)
    assert "⚠️" in warning_line and "Warning:" in warning_line
    assert isinstance(suggestion_line, str)
    assert "migrating data manually" in suggestion_line
