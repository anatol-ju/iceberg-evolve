# iceberg_evolve/renderer.py

from rich.console import Group, Console
from rich.tree import Tree
from rich.text import Text

from iceberg_evolve.diff import SchemaDiff, FieldChange
from iceberg_evolve.utils import clean_type_str
from pyiceberg.types import StructType, ListType, MapType

class SchemaDiffRenderer:
    def __init__(self, diff: SchemaDiff, console: Console | None = None):
        self.diff = diff
        self.console = console or Console()

    def display(self) -> None:
        trees: list[Tree | Text] = []

        for section, changes in self.diff:
            if not changes:
                continue

            if section == "changed":
                # any change with no dot is top-level
                top_level = {c.name for c in changes if "." not in c.name}
                # drop nested changes when their parent is already in top_level
                changes_to_render = [
                    c for c in changes
                    if "." not in c.name or c.name.split(".", 1)[0] not in top_level
                ]
            else:
                changes_to_render = changes
            # 1) Build the section header
            color = {"added":"green","removed":"red","changed":"yellow"}[section]
            header = Tree(f"[bold {color}]{section.upper()}[/bold {color}]")

            # 2) Populate header with each change node
            for change in changes_to_render:
                node = self._render_change(section, change)
                header.add(node)

            trees.append(header)
            trees.append(Text())  # blank line

        # strip trailing blank
        if trees and isinstance(trees[-1], Text):
            trees.pop()

        # 3) Print them all
        self.console.print(Group(*trees))

    def _render_change(self, section: str, change: FieldChange) -> Tree:
        """
        Render one FieldChange into a Tree node (including nested 'from'/'to').
        """
        color = {"added":"green3","removed":"red1","changed":"yellow1"}[section]
        symbol = {"added":"+","removed":"-","changed":"~"}[section]

        # root label: e.g. "+ id: int"
        if section == "added":
            label = f"{symbol} {change.name}: {clean_type_str(change.new_type)}"
        elif section == "removed":
            label = f"{symbol} {change.name}"
        else:  # changed
            label = f"{symbol} {change.previous_name or change.name}"
        node = Tree(f"[{color}]{label}[/{color}]")

        # now drill into the specific change kind
        if change.change == "renamed":
            node.add(f"renamed to: [yellow1]{change.name}[/yellow1]")
        elif change.change == "type_changed":
            from_node = node.add("from:")
            self._walk_and_color(from_node, change.current_type, side="from", base=change.name)

            to_node   = node.add("to:")
            self._walk_and_color(to_node,   change.new_type,   side="to",   base=change.name)
        # … handle doc_changed, moved similarly …

        return node

    def _walk_and_color(self, tree: Tree, typ, side: str, base: str) -> None:
        """
        Recursively walk a StructType or ListType-of-StructType,
        colorizing leaves based on added/removed/type_changed in self.diff.
        """
        if isinstance(typ, StructType):
            for f in typ.fields:
                path = f"{base}.{f.name}"
                style = None
                if side == "from" and any(c.name == path and c.change == "removed" for c in self.diff.removed):
                    style = "red"
                elif side == "to" and any(c.name == path and c.change == "added" for c in self.diff.added):
                    style = "green"
                elif side == "to" and any(c.name == path and c.change == "type_changed" for c in self.diff.changed):
                    style = "yellow"

                field_type = f.field_type
                # StructType field
                if isinstance(field_type, StructType):
                    lbl = f"{f.name}: struct"
                    child = tree.add(f"[{style}]{lbl}[/{style}]" if style else lbl)
                    self._walk_and_color(child, field_type, side, path)
                # ListType of StructType
                elif isinstance(field_type, ListType) and isinstance(field_type.element_type, StructType):
                    lbl = f"{f.name}: list<struct>"
                    child = tree.add(f"[{style}]{lbl}[/{style}]" if style else lbl)
                    self._walk_and_color(child, field_type.element_type, side, path)
                # ListType of primitive
                elif isinstance(field_type, ListType):
                    elem = field_type.element_type
                    lbl = f"{f.name}: list<{clean_type_str(elem)}>"
                    tree.add(f"[{style}]{lbl}[/{style}]" if style else lbl)
                # Primitive or other types
                else:
                    lbl = f"{f.name}: {clean_type_str(field_type)}"
                    tree.add(f"[{style}]{lbl}[/{style}]" if style else lbl)
        else:
            # leaf: primitive or list<int>, just inline
            tree.add(clean_type_str(typ))
