from pyiceberg.types import ListType, StructType
from rich.console import Console, Group
from rich.text import Text
from rich.tree import Tree

from iceberg_evolve.diff import FieldChange, SchemaDiff
from iceberg_evolve.migrate import UpdateColumn
from iceberg_evolve.utils import clean_type_str


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
            label = f"[{color}]{symbol} {change.name}[/{color}]: {clean_type_str(change.new_type)}"
        elif section == "removed":
            label = f"[{color}]{symbol} {change.name}[/{color}]"
        else:  # changed
            label = f"[{color}]{symbol} {change.previous_name or change.name}[/{color}]"
        node = Tree(label)

        # now drill into the specific change kind
        if change.change == "renamed":
            node.add(f"renamed to: [yellow1]{change.name}[/yellow1]")
        elif change.change == "type_changed":
            # Render 'from'
            ct = change.current_type
            if isinstance(ct, StructType) or (isinstance(ct, ListType) and isinstance(ct.element_type, StructType)):
                from_node = node.add("from:")
                self._walk_and_color(from_node, ct, side="from", base=change.name)
            else:
                node.add(f"from: {clean_type_str(ct)}")

            # Render 'to'
            nt = change.new_type
            if isinstance(nt, StructType) or (isinstance(nt, ListType) and isinstance(nt.element_type, StructType)):
                to_node = node.add("to:")
                self._walk_and_color(to_node, nt, side="to", base=change.name)
            else:
                node.add(f"to: {clean_type_str(nt)}")
        elif change.change == "doc_changed":
            node.add("[yellow1]doc changed[/yellow1]")
        elif change.change == "moved":
            node.add(f"moved {change.position}: [yellow1]{change.relative_to}[/yellow1]")

        return node

    def _walk_and_color(self, tree: Tree, typ, side: str, base: str) -> None:
        """
        Recursively walk a StructType or ListType-of-StructType,
        colorizing leaves based on added/removed/type_changed in self.diff.
        """
        if isinstance(typ, StructType):
            for f in typ.fields:
                path = f"{base}.{f.name}"
                required_str = " required" if f.required else ""
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
                    lbl = f"{f.name}: struct{required_str}"
                    child = tree.add(f"[{style}]{lbl}[/{style}]" if style else lbl)
                    self._walk_and_color(child, field_type, side, path)
                # ListType of StructType
                elif isinstance(field_type, ListType) and isinstance(field_type.element_type, StructType):
                    lbl = f"{f.name}: list<struct>{required_str}"
                    child = tree.add(f"[{style}]{lbl}[/{style}]" if style else lbl)
                    self._walk_and_color(child, field_type.element_type, side, path)
                # ListType of primitive
                elif isinstance(field_type, ListType):
                    elem = field_type.element_type
                    lbl = f"{f.name}: list<{clean_type_str(elem)}>{required_str}"
                    tree.add(f"[{style}]{lbl}[/{style}]" if style else lbl)
                # Primitive or other types
                else:
                    lbl = f"{f.name}: {clean_type_str(field_type)}{required_str}"
                    tree.add(f"[{style}]{lbl}[/{style}]" if style else lbl)
        else:
            # leaf: primitive or list<int>, just inline
            tree.add(clean_type_str(typ))


class EvolutionOperationsRenderer:
    def __init__(self, ops, console: Console | None = None):
        self.ops = [op for op in ops if "." not in op.name]  # filter nested
        self.console = console or Console()

    def display(self) -> None:
        """
        Render evolution operations with nested colored diffs for UpdateColumn.
        """
        prev_type = None
        has_unsupported = False
        for op in self.ops:
            current_type = type(op)
            # blank line between different operation types
            if prev_type and current_type is not prev_type:
                self.console.print()
            prev_type = current_type

            # Print the operation tree
            self.console.print(op.pretty(use_color=True))

            # For UpdateColumn, display nested SchemaDiff tree
            if isinstance(op, UpdateColumn) and getattr(op, "nested_changes", None):
                diff = SchemaDiff(added=[], removed=[], changed=op.nested_changes)
                SchemaDiffRenderer(diff, self.console).display()

            if not op.is_supported:
                has_unsupported = True

        if has_unsupported:
            self.console.print(
                "\n[bold yellow]⚠️  Warning:[/bold yellow] Some operations are not supported (yet) and will be skipped."
            )
            self.console.print(
                "Consider adding new columns with the desired structure and migrating data manually."
            )
            self.console.print(
                "[bold dark_orange3]Always compare the applied schema with the expected to ensure correctness.[/bold dark_orange3]\n"
            )
