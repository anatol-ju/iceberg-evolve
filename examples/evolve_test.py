import json
import os

from iceberg_evolve.diff import diff_schemas
from iceberg_evolve.migrate import generate_evolution_operations
from iceberg_evolve.schema import Schema


def load_schema(path: str) -> Schema:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return Schema(schema=data)

def main():
    base_path = os.path.join(os.path.dirname(__file__), "..", "examples")
    current_schema = load_schema(os.path.join(base_path, "users_current.json"))
    new_schema = load_schema(os.path.join(base_path, "users_new.json"))

    diff = diff_schemas(current_schema, new_schema)

    print("\nSchema differences:")
    diff.display(use_color=True)


    operations = generate_evolution_operations(diff)
    print("\nEvolution operations:")
    for op in operations:
        op.display()

if __name__ == "__main__":
    main()