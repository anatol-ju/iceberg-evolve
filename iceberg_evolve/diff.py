from dataclasses import dataclass, fields

from pyiceberg.types import IcebergType, NestedField, StructType
from rich.console import Console

from iceberg_evolve.migrate import (
    AddColumn,
    BaseEvolutionOperation,
    DropColumn,
    MoveColumn,
    RenameColumn,
    UpdateColumn
)
from iceberg_evolve.utils import types_equivalent


@dataclass
class FieldChange:
    """
    Represents a change in a field between two schemas.
    This can be an addition, removal, rename, move, doc or type change.

    Attributes:
        name (str): The name of the field (new name for renamed fields).
        current_type (IcebergType | None): The current type of the field.
        new_type (IcebergType | None): The new type of the field.
        doc (str | None): Documentation string for the field.
        change (str): One of "added", "removed", "type_changed", "doc_changed", "renamed", "moved".
        previous_name (str | None): Original name if this is a rename.
        position (str | None): "first", "before", or "after" for moved fields.
        relative_to (str | None): Field this one is moved relative to.
    """
    name: str
    change: str  # "added", "removed", "type_changed", "doc_changed", "renamed", "moved"
    current_type: IcebergType | None = None
    new_type: IcebergType | None = None
    doc: str | None = None
    previous_name: str | None = None  # For renamed columns
    position: str | None = None       # For moved columns ("before", "after", "first")
    relative_to: str | None = None    # Target column for moved columns

    def pretty(self) -> str:
        from iceberg_evolve.utils import clean_type_str
        if self.change == "added":
            return f"{self.name}: {clean_type_str(self.new_type)}"
        elif self.change == "removed":
            return self.name
        elif self.change == "type_changed":
            ct = clean_type_str(self.current_type)
            nt = clean_type_str(self.new_type)
            return f"{self.name}:\n  from: {ct}\n    to: {nt}"
        elif self.change == "doc_changed":
            return f"{self.name}: doc changed"
        elif self.change == "renamed":
            return f"{self.previous_name} renamed to {self.name}"
        elif self.change == "moved":
            pos = self.position or ""
            rel = self.relative_to or ""
            return f"{self.name} moved {pos} {rel}".strip()
        else:
            return str(self)

@dataclass
class SchemaDiff:
    """
    Represents the differences between two schemas.
    Contains lists of added, removed, and changed fields.

    Attributes:
        added (list[FieldChange]): Fields added in the new schema.
        removed (list[FieldChange]): Fields removed from the current schema.
        changed (list[FieldChange]): Fields changed between the schemas.
    """
    added: list[FieldChange]
    removed: list[FieldChange]
    changed: list[FieldChange]

    def __iter__(self):
        for f in fields(self):
            yield f.name, getattr(self, f.name)

    def __str__(self) -> str:
        """
        Return a plain-text representation of the schema diff for debugging or logging.
        """
        lines = []

        for section, changes in self:
            if not changes:
                continue
            lines.append(f"{section.upper()}:")
            for change in changes:
                lines.append(f"  - {change.pretty()}")
            lines.append("")

        return "\n".join(lines)

    def display(self, console: Console | None = None) -> None:
        # delegate all rendering
        from iceberg_evolve.renderer import SchemaDiffRenderer
        SchemaDiffRenderer(self, console).display()

    @staticmethod
    def from_schemas(current: "Schema", new: "Schema") -> "SchemaDiff":
        """
        Create a SchemaDiff from two Schema objects.

        Args:
            current (Schema): The current schema.
            new (Schema): The new schema to compare against.

        Returns:
            SchemaDiff: The differences between the two schemas.
        """
        from pyiceberg.schema import Schema as PyIcebergSchema
        from iceberg_evolve.schema import Schema as EvolveSchema
        if not isinstance(current, (EvolveSchema, PyIcebergSchema)) or not isinstance(new, (EvolveSchema, PyIcebergSchema)):
            raise ValueError("Both current and new must be instances of either iceberg_evolve.schema.Schema or pyiceberg.schema.Schema.")

        added = []
        removed = []
        changed = []

        def _diff_fields(
            current_fields: dict[int, NestedField],
            new_fields: dict[int, NestedField],
            current_order: list[int],
            new_order: list[int],
            parent_path: str = "",
        ):
            for field_id, new_field in new_fields.items():
                path = f"{parent_path}.{new_field.name}" if parent_path else new_field.name
                if field_id not in current_fields:
                    added.append(FieldChange(name=path, new_type=new_field.field_type, doc=new_field.doc, change="added"))
                else:
                    current_field = current_fields[field_id]

                    # Detect renames
                    if current_field.name != new_field.name:
                        changed.append(FieldChange(
                            name=path,
                            previous_name=current_field.name,
                            current_type=current_field.field_type,
                            new_type=new_field.field_type,
                            doc=new_field.doc,
                            change="renamed"
                        ))

                    # Detect type changes (using canonical equivalence)
                    if not types_equivalent(current_field.field_type, new_field.field_type):
                        changed.append(FieldChange(
                            name=path,
                            current_type=current_field.field_type,
                            new_type=new_field.field_type,
                            doc=new_field.doc,
                            change="type_changed"
                        ))

                    # Detect doc changes
                    if current_field.doc != new_field.doc:
                        changed.append(FieldChange(
                            name=path,
                            current_type=current_field.field_type,
                            new_type=new_field.field_type,
                            doc=new_field.doc,
                            change="doc_changed"
                        ))

                    # Recurse into structs
                    if (
                        isinstance(current_field.field_type, StructType) and
                        isinstance(new_field.field_type, StructType)
                    ):
                        _diff_fields(
                            {f.field_id: f for f in current_field.field_type.fields},
                            {f.field_id: f for f in new_field.field_type.fields},
                            [f.field_id for f in current_field.field_type.fields],
                            [f.field_id for f in new_field.field_type.fields],
                            parent_path=path
                        )
            # Only flag top-level fields whose index changed
            if parent_path == "":
                def minimal_moves(orig: list[int], new: list[int]) -> list[int]:
                    if orig == new:
                        return []
                    common = [fid for fid in orig if fid in new]
                    common_sorted = sorted(common, key=new.index)
                    from difflib import SequenceMatcher
                    matcher = SequenceMatcher(None, common, common_sorted)
                    moved = []
                    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
                        if tag != "equal":
                            moved.extend(common[i1:i2])
                    return moved

                moved_ids = minimal_moves(current_order, new_order)
            else:
                moved_ids = []
            for field_id in moved_ids:
                name = new_fields[field_id].name
                path = f"{parent_path}.{name}" if parent_path else name
                i = new_order.index(field_id)
                changed.append(FieldChange(
                    name=path,
                    change="moved",
                    position="after" if i > 0 else "first",
                    relative_to=new_fields[new_order[i - 1]].name if i > 0 else None
                ))

            for field_id, current_field in current_fields.items():
                if field_id not in new_fields:
                    path = f"{parent_path}.{current_field.name}" if parent_path else current_field.name
                    removed.append(FieldChange(name=path, current_type=current_field.field_type, doc=current_field.doc, change="removed"))

        _diff_fields(
            {f.field_id: f for f in current.fields},
            {f.field_id: f for f in new.fields},
            [f.field_id for f in current.fields],
            [f.field_id for f in new.fields]
        )

        return SchemaDiff(added=added, removed=removed, changed=changed)

    @staticmethod
    def union_by_name(current: "Schema", new: "Schema") -> "SchemaDiff":
        """
        Create a SchemaDiff that represents the union of two schemas by name.
        This is useful for cases where you want to merge schemas without considering IDs.

        Args:
            current (Schema): The current schema.
            new (Schema): The new schema to compare against.

        Returns:
            SchemaDiff: The differences between the two schemas, treating them as unions by name.
        """
        # Map by name, ignoring IDs
        current_by_name = {f.name: f for f in current.fields}
        new_by_name     = {f.name: f for f in new.fields}

        added, changed = [], []

        # Added fields (in new only)
        for name, nf in new_by_name.items():
            if name not in current_by_name:
                added.append(FieldChange(
                    name=name,
                    new_type=nf.field_type,
                    change="added"
                ))

        # Updates for fields in both
        for name in new_by_name.keys() & current_by_name.keys():
            cf, nf = current_by_name[name], new_by_name[name]
            if not types_equivalent(cf.field_type, nf.field_type):
                # You could choose UpdateColumn or a UnionSchema op here
                changed.append(FieldChange(
                    name=name,
                    current_type=cf.field_type,
                    new_type=nf.field_type,
                    change="type_changed"
                ))

        # No removals for union!
        diff = SchemaDiff(added=added, removed=[], changed=changed)
        # mark that this diff came via union_by_name
        diff._union_by_name = True
        return diff

    def to_evolution_operations(self) -> list[BaseEvolutionOperation]:
        """
        Convert this SchemaDiff into a list of evolution operations in a dependency-safe order.
        This means renames and updates will be applied before adds, drops, and moves.
        By applying this strategy, we ensure that the schema evolution is safe and respects dependencies.

        If this is not enforced, a scenario could arise where, for example, a column is moved before
        it is renamed, therefore causing the move to reference the old name, which would fail.

        Returns:
            list[BaseEvolutionOperation]: A list of evolution operations to apply, sorted by priority.
        """
        # If the user called union_by_name(), warn and fall back to individual adds/updates
        if getattr(self, "_union_by_name", False):
            Console().print(
                "[bold yellow]Warning:[/bold yellow] union_by_name() does not emit a single UnionSchema evolution operation yet;"
                " performing individual ADD/UPDATE operations instead."
            )
        ops: list[BaseEvolutionOperation] = []

        # 1) Renames
        for fc in self.changed:
            if fc.change == "renamed":
                ops.append(RenameColumn(name=fc.previous_name or "", target=fc.name))

        # 2) Type & doc updates
        for fc in self.changed:
            if fc.change in ("type_changed", "doc_changed"):
                ops.append(
                    UpdateColumn(
                        name=fc.name,
                        current_type=fc.current_type,
                        new_type=fc.new_type,
                        doc=fc.doc
                    )
                )

        # 3) Adds
        for fc in self.added:
            ops.append(AddColumn(name=fc.name, new_type=fc.new_type, doc=fc.doc))

        # 4) Drops
        for fc in self.removed:
            ops.append(DropColumn(name=fc.name))

        # 5) Moves
        for fc in self.changed:
            if fc.change == "moved":
                ops.append(MoveColumn(
                    name=fc.name,
                    target=fc.relative_to or "",
                    position=fc.position or "after"
                ))

        return ops

