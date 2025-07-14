from dataclasses import dataclass

from pyiceberg.table import Table
from pyiceberg.types import IcebergType
from rich.console import Console
from rich.text import Text

from iceberg_evolve.diff import SchemaDiff
from iceberg_evolve.utils import clean_type_str, is_narrower_than


@dataclass
class EvolutionOperation:
    op_type: str            # e.g. "add_column", "drop_column", "update_column_type"
    name: str               # column path, e.g. "metadata.login_attempts"
    current_type: IcebergType | None = None
    new_type: IcebergType | None = None
    doc: str | None = None
    target: str | None = None    # for rename or move

    def to_serializable_dict(self) -> dict:
        return {
            "operation": self.op_type,
            "name": self.name,
            **({"from": clean_type_str(self.current_type)} if self.current_type else {}),
            **({"to": clean_type_str(self.new_type)} if self.new_type else {}),
            **({"doc": self.doc} if self.doc else {}),
            **({"target": self.target} if self.target else {}),
        }

    def pretty(self, use_color: bool = False) -> Text:
        """
        Returns a pretty string representation of the operation.
        Only the operation header is colored if use_color is True.
        If use_color is True, to color format the output.
        Set it to False to return a plain string.

        Args:
            use_color (bool): Whether to use color formatting in the output.

        Returns:
            Text: A rich Text object with the formatted operation.

        Example:
            >>> op = EvolutionOperation(op_type="add_column", name="new_col", new_type=IcebergType("string"))
            >>> print(op.pretty(use_color=True))
            ADD COLUMN
              new_col: string
        """
        styles = {
            "add_column": (
                "ADD COLUMN",
                "green bold",
                f"  {self.name}: {clean_type_str(self.new_type)}"
            ),
            "drop_column": (
                "DROP COLUMN",
                "red bold",
                f"  {self.name}"
            ),
            "update_column_type": (
                "UPDATE COLUMN",
                "yellow bold",
                f"  {self.name}\n  from: {clean_type_str(self.current_type)}\n    to: {clean_type_str(self.new_type)}"
            ),
            "rename_column": (
                "RENAME COLUMN",
                "cyan bold",
                f"  {self.name}\n  to: {self.target}"
            ),
            "move_column": (
                "MOVE COLUMN",
                "magenta bold",
                f"  from: {self.name} ({self.doc})\n   of: {self.target}"
            ),
            "union_schema": (
                "UNION SCHEMA:",
                "blue bold",
                f"  with type: {clean_type_str(self.new_type)}"
            )
        }

        label, style, body = styles.get(
            self.op_type,
            (f"Unknown operation: {self.op_type}", "white", "")
        )

        if use_color:
            return Text.assemble((f"\n{label}\n", style), body)
        else:
            return Text.assemble(f"\n{label}\n", body)

    def display(self, use_color: bool = True, console: Console | None = None) -> None:
        """
        Pretty print the schema diff using rich formatting.

        Args:
            use_color (bool): Whether to apply color styling.
            console (Console | None): Optional rich Console to use for output.
        """
        console = console or Console()
        console.print(self.pretty(use_color=use_color))

    def is_breaking(self) -> bool:
        """
        Returns True if this operation is likely to break downstream readers/writers
        without special allowances. E.g. dropping a column or narrowing a type.
        """
        if self.op_type == "drop_column":
            return True
        if self.op_type == "update_column_type":
            # Narrowing types is generally breaking
            if self.current_type and self.new_type:
                return is_narrower_than(self.new_type, self.current_type)
        return False

def generate_evolution_operations(diff: SchemaDiff) -> list[EvolutionOperation]:
    """
    Convert a schema diff into a list of schema evolution operations.

    Args:
        diff (SchemaDiff): The SchemaDiff returned by `compare_schemas()`.

    Returns:
        list[dict]: A list of evolution operation dictionaries.
    """
    operations: list[EvolutionOperation] = []

    changed_parents = {f.name for f in diff.changed if "." in f.name}
    def is_nested_under_changed(n: str) -> bool:
        return any(n.startswith(p + ".") for p in changed_parents)

    for f in diff.added:
        if not is_nested_under_changed(f.name):
            operations.append(EvolutionOperation(
                op_type="add_column",
                name=f.name,
                new_type=f.new_type,
                doc=getattr(f, "doc", None)
            ))

    for f in diff.removed:
        operations.append(EvolutionOperation(
            op_type="drop_column",
            name=f.name
        ))

    for f in diff.changed:
        operations.append(EvolutionOperation(
            op_type="update_column_type",
            name=f.name,
            current_type=f.current_type,
            new_type=f.new_type,
            doc=getattr(f, "doc", None),
            target=getattr(f, "target", None)
        ))

    return operations


def apply_evolution_operations(
    table: Table,
    operations: list[EvolutionOperation],
    validate: bool = True,
    allow_incompatible: bool = False
) -> None:
    """
    Apply a list of schema evolution operations to an Iceberg table.

    Args:
        table (pyiceberg.table.Table): The Iceberg table to update.
        operations (list[EvolutionOperation]): A list of evolution operation directives.
        validate (bool): Whether to validate the operations before applying. Defaults to True.
        allow_incompatible (bool): Whether to allow incompatible changes like dropping columns. Defaults to False.
    """
    with table.update_schema(allow_incompatible_changes=allow_incompatible) as update:
        for op in operations:
            path = tuple(op.name.split(".")) if "." in op.name else op.name

            if op.op_type == "add_column":
                update.add_column(path, op.new_type, doc=op.doc)

            elif op.op_type == "drop_column":
                update.delete_column(path)

            elif op.op_type == "update_column_type":
                update.update_column(path, field_type=op.new_type)

            elif op.op_type == "rename_column":
                update.rename_column(path, op.target)

            elif op.op_type == "move_column":
                direction = op.doc  # Assuming direction is stored in doc or target, unclear from original
                if direction == "first":
                    update.move_first(path)
                elif direction == "after":
                    update.move_after(path, op.target)
                elif direction == "before":
                    update.move_before(path, op.target)

            elif op.op_type == "union_schema":
                update.union_by_name(op.new_type)
