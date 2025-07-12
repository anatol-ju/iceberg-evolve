from dataclasses import dataclass, fields
from typing import List, Optional

from pyiceberg.types import IcebergType
from iceberg_evolve.utils import parse_struct_type, from_json_type

from schemaworks import JsonSchemaConverter

from iceberg_evolve.schema import Schema


@dataclass
class FieldChange:
    name: str
    current_type: Optional[IcebergType] = None
    new_type: Optional[IcebergType] = None


@dataclass
class SchemaDiff:
    added: List[FieldChange]
    removed: List[FieldChange]
    changed: List[FieldChange]

    def __iter__(self):
        for f in fields(self):
            yield f.name, getattr(self, f.name)


def diff_schemas(current: Schema, new: Schema) -> SchemaDiff:
    """
    Compare two Schema objects and return a SchemaDiff instance.
    """
    current_converter = JsonSchemaConverter(current.schema)
    new_converter = JsonSchemaConverter(new.schema)
    current_dtypes = current_converter.to_dtypes()
    new_dtypes = new_converter.to_dtypes()

    # Determine added fields
    added = [
        FieldChange(name=k, new_type=from_json_type(new_dtypes[k]))
        for k in set(new_dtypes) - set(current_dtypes)
    ]

    # Determine removed fields
    removed = [
        FieldChange(name=k, current_type=from_json_type(current_dtypes[k]))
        for k in set(current_dtypes) - set(new_dtypes)
    ]

    # Determine changed fields
    changed = []
    for k in set(current_dtypes) & set(new_dtypes):
        old_type = current_dtypes[k]
        new_type = new_dtypes[k]
        if old_type != new_type:
            changed.append(FieldChange(
                name=k,
                current_type=from_json_type(old_type),
                new_type=from_json_type(new_type)
            ))

    return SchemaDiff(added=added, removed=removed, changed=changed)
