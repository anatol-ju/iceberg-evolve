from dataclasses import dataclass, fields

from pyiceberg.types import IcebergType
from rich.console import Console
from rich.text import Text
from schemaworks import JsonSchemaConverter

from iceberg_evolve.schema import Schema
from iceberg_evolve.utils import parse_sql_type


@dataclass
class FieldChange:
    """
    Represents a change in a field between two schemas.
    This can be an addition, removal, or type change.
    """
    name: str
    current_type: IcebergType | None = None
    new_type: IcebergType | None = None
    doc: str | None = None
    target: str | None = None

    def pretty(self, change_type: str) -> str:
        from iceberg_evolve.utils import clean_type_str
        if change_type == "added":
            return f"{self.name}: {clean_type_str(self.new_type)}"
        elif change_type == "removed":
            return self.name
        elif change_type == "changed":
            ct = clean_type_str(self.current_type)
            nt = clean_type_str(self.new_type)
            return f"{self.name}:\n  from: {ct}\n    to: {nt}"
        else:
            return str(self)

@dataclass
class SchemaDiff:
    """
    Represents the differences between two schemas.
    Contains lists of added, removed, and changed fields.
    """
    added: list[FieldChange]
    removed: list[FieldChange]
    changed: list[FieldChange]

    def __iter__(self):
        for f in fields(self):
            yield f.name, getattr(self, f.name)

    def pretty(self, use_color: bool = False) -> Text:
        """
        Returns a pretty Text representation of the schema diff.
        """
        color_map = {
            "added": "green",
            "removed": "red",
            "changed": "yellow"
        }
        lines = []
        for section, changes in self:
            if use_color:
                header = Text.assemble((f"\n{section.upper()}:", color_map.get(section, "")))
            else:
                header = Text(f"\n{section.upper()}:")
            lines.append(header)
            for change in changes:
                line_text = change.pretty(change_type=section)
                if use_color:
                    lines.append(Text(f"  {line_text}"))
                else:
                    lines.append(Text(f"  {line_text}"))
        return Text("\n").join(lines)

    def display(self, use_color: bool = True, console: Console | None = None) -> None:
        """
        Pretty print the schema diff using rich formatting.

        Args:
            use_color (bool): Whether to apply color styling.
            console (Console | None): Optional rich Console to use for output.
        """
        console = console or Console()
        console.print(self.pretty(use_color=use_color))

def _add_type_change_if_different(changes, path, type_from, type_to):
    if not any(c["name"] == path and c["type_from"] == type_from and c["type_to"] == type_to for c in changes["type_change"]):
        changes["type_change"].append({
            "name": path,
            "type_from": type_from,
            "type_to": type_to
        })

def _merge_changes(base_changes, nested_changes):
    for key in base_changes:
        for entry in nested_changes[key]:
            if entry not in base_changes[key]:
                base_changes[key].append(entry)

def _resolve_struct_type(item_schema: dict) -> str:
    if item_schema.get("type") == "object" and "properties" in item_schema:
        fields = [f"{k}: {v.get('type')}" for k, v in item_schema["properties"].items()]
        return f"struct<{', '.join(fields)}>"
    return item_schema.get("type", "unknown")

def _diff_schemas(current_schema: dict, new_schema: dict, current_dtypes: dict[str, str], new_dtypes: dict[str, str], path: str = "") -> dict:
    """
    Compare two JSON schemas recursively.

    Args:
        current_schema (dict): The original schema to compare against.
        new_schema (dict): The new schema to compare with.
        path (str): The current path in the schema for nested properties.

    Returns:
        dict: A dictionary with 'add', 'remove', and 'type_change' keys,
              each containing a list of changes.
    """

    changes = {"add": [], "remove": [], "type_change": []}

    old_props = current_schema.get("properties", {})
    new_props = new_schema.get("properties", {})

    # Detect additions and recursive diffs
    for prop, new_field in new_props.items():
        full_name = f"{path}.{prop}" if path else prop
        if prop not in old_props:
            if path == "":
                # Only treat as add if it's a top-level field
                changes["add"].append({
                    "name": full_name,
                    "type": new_field.get("type")
                })
            else:
                # Treat as a type change to the parent field
                parent_type_from = current_dtypes.get(path, _resolve_struct_type(current_schema))
                parent_type_to = new_dtypes.get(path, _resolve_struct_type(new_schema))
                _add_type_change_if_different(changes, path, parent_type_from, parent_type_to)
                continue
        else:
            # Present in both: check type
            old_field = old_props[prop]
            old_type = old_field.get("type")
            new_type = new_field.get("type")
            if old_type != new_type:
                if old_type == "object" and "properties" in old_field and new_type == "object" and "properties" in new_field:
                    type_from = _resolve_struct_type(old_field)
                    type_to = _resolve_struct_type(new_field)
                else:
                    type_from = current_dtypes.get(full_name, old_type)
                    type_to = new_dtypes.get(full_name, new_type)
                _add_type_change_if_different(changes, full_name, type_from, type_to)

            # Recurse into nested objects
            if new_type == "object":
                nested = _diff_schemas(old_field, new_field, current_dtypes, new_dtypes, full_name)
                _merge_changes(changes, nested)

            # Compare array items
            if new_type == "array":
                old_items = old_field.get("items", {})
                new_items = new_field.get("items", {})
                item_name = f"{full_name}[]"  # Leave this as-is for nested diff tracking
                old_item_type = old_items.get("type")
                new_item_type = new_items.get("type")

                if old_item_type and new_item_type and old_item_type != new_item_type:
                    type_from = f"array<{_resolve_struct_type(old_items)}>"
                    type_to = f"array<{_resolve_struct_type(new_items)}>"
                    _add_type_change_if_different(changes, full_name, type_from, type_to)
                else:
                    if new_item_type == "object":
                        nested = _diff_schemas(old_items, new_items, current_dtypes, new_dtypes, item_name)
                        _merge_changes(changes, nested)

    # Detect removals
    for prop in old_props:
        if prop not in new_props:
            full_name = f"{path}.{prop}" if path else prop
            if path == "":
                # Only treat as remove if it's a top-level field
                changes["remove"].append({"name": full_name})
            else:
                # Treat as a type change to the parent field
                parent_type_from = current_dtypes.get(path, _resolve_struct_type(current_schema))
                parent_type_to = new_dtypes.get(path, _resolve_struct_type(new_schema))
                _add_type_change_if_different(changes, path, parent_type_from, parent_type_to)

    return changes


def diff_schemas(current: Schema, new: Schema) -> SchemaDiff:
    """
    Compare two Schema objects and return a SchemaDiff instance.
    """
    current_converter = JsonSchemaConverter(current.schema)
    new_converter = JsonSchemaConverter(new.schema)
    current_dtypes = current_converter.to_dtypes()
    new_dtypes = new_converter.to_dtypes()
    raw = _diff_schemas(current.schema, new.schema, current_dtypes, new_dtypes, "")

    # Convert added
    added = []
    for entry in raw.get("add", []):
        type_str = entry.get("type")
        type_obj = parse_sql_type(type_str) if type_str else None
        added.append(FieldChange(name=entry["name"], new_type=type_obj))

    # Convert removed
    removed = []
    for entry in raw.get("remove", []):
        type_str = current_dtypes.get(entry["name"])
        type_obj = parse_sql_type(type_str) if type_str else None
        removed.append(FieldChange(name=entry["name"], current_type=type_obj))

    # Convert changed
    changed = []
    for entry in raw.get("type_change", []):
        ct_obj = parse_sql_type(entry.get("type_from")) if entry.get("type_from") else None
        nt_obj = parse_sql_type(entry.get("type_to")) if entry.get("type_to") else None
        changed.append(FieldChange(
            name=entry["name"],
            current_type=ct_obj,
            new_type=nt_obj,
            doc=entry.get("doc"),
            target=entry.get("target")
        ))

    return SchemaDiff(added=added, removed=removed, changed=changed)
