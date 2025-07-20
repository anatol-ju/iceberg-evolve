import json
import os

from pyiceberg.schema import Schema

from iceberg_evolve.diff import SchemaDiff
from iceberg_evolve.utils import IcebergSchemaSerializer


def load_schema(path: str) -> Schema:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return IcebergSchemaSerializer.from_dict(data)

def main():
    base_path = os.path.join(os.path.dirname(__file__), "..", "examples")
    current_schema_path = os.path.join(base_path, "users_current_serialized.json")
    new_schema_path = os.path.join(base_path, "users_new_serialized.json")

    current_schema = load_schema(current_schema_path)
    new_schema = load_schema(new_schema_path)

    # Generate diff
    diff = SchemaDiff.from_schemas(current_schema, new_schema)

    print("\nSchema differences:")
    diff.display()

    # operations = diff.to_evolution_operations()
    # print("\nEvolution operations:")
    # for op in operations:
    #     op.display()

if __name__ == "__main__":
    main()
