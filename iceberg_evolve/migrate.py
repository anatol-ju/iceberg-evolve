from dataclasses import dataclass

from pyiceberg.table import Table
from pyiceberg.types import IcebergType

from iceberg_evolve.diff import SchemaDiff
from iceberg_evolve.utils import is_narrower_than, clean_type_str


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

    def pretty(self) -> str:
        """
        Returns a pretty string representation of the operation.
        """
        if self.op_type == "add_column":
            return f"\nADD\n  {self.name}: {clean_type_str(self.new_type)}"
        elif self.op_type == "drop_column":
            return f"\nDROP\n  {self.name}"
        elif self.op_type == "update_column_type":
            return f"\nUPDATE\n  {self.name}\n  from: {clean_type_str(self.current_type)}\n    to: {clean_type_str(self.new_type)}"
        elif self.op_type == "rename_column":
            return f"\nRENAME\n  {self.name}\n  to: {self.target}"
        elif self.op_type == "move_column":
            return f"\nMOVE\n  from: {self.name} ({self.doc})\n   of: {self.target}"
        elif self.op_type == "union_schema":
            return f"\nUNION schema:\n  with type: {clean_type_str(self.new_type)}"
        else:
            return f"\nUnknown operation: {self.op_type}"

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
            new_type=f.new_type
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
