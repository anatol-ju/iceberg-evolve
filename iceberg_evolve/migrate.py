from pyiceberg.table import Table

from iceberg_evolve.diff import SchemaDiff
from iceberg_evolve.evolution_operation import (
    AddColumn,
    BaseEvolutionOperation,
    DropColumn,
    UpdateColumn
)


def generate_evolution_operations(diff: SchemaDiff) -> list[BaseEvolutionOperation]:
    """
    Convert a schema diff into a list of schema evolution operations.

    Args:
        diff (SchemaDiff): The SchemaDiff returned by `compare_schemas()`.

    Returns:
        list[dict]: A list of evolution operation dictionaries.
    """
    operations: list[BaseEvolutionOperation] = []

    changed_parents = {f.name for f in diff.changed if "." in f.name}
    def is_nested_under_changed(n: str) -> bool:
        return any(n.startswith(p + ".") for p in changed_parents)

    for f in diff.added:
        if not is_nested_under_changed(f.name):
            operations.append(AddColumn(
                name=f.name,
                new_type=f.new_type,
                doc=getattr(f, "doc", None)
            ))

    for f in diff.removed:
        operations.append(DropColumn(
            name=f.name
        ))

    for f in diff.changed:
        operations.append(UpdateColumn(
            name=f.name,
            current_type=f.current_type,
            new_type=f.new_type,
            doc=getattr(f, "doc", None)
        ))

    return operations


def apply_evolution_operations(
    table: Table,
    operations: list[BaseEvolutionOperation],
    validate: bool = True,
    allow_incompatible: bool = False
) -> None:
    """
    Apply a list of schema evolution operations to an Iceberg table.

    Args:
        table (pyiceberg.table.Table): The Iceberg table to update.
        operations (list[BaseEvolutionOperation]): A list of evolution operation directives.
        validate (bool): Whether to validate the operations before applying. Defaults to True.
        allow_incompatible (bool): Whether to allow incompatible changes like dropping columns. Defaults to False.
    """
    with table.update_schema(allow_incompatible_changes=allow_incompatible) as update:
        for op in operations:
            if validate:
                if not op.is_breaking() or allow_incompatible:
                    op.apply(update)
            else:
                op.apply(update)
