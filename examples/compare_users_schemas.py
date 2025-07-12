from iceberg_evolve.schema import Schema
from iceberg_evolve.diff import diff_schemas
from iceberg_evolve.migrate import generate_evolution_operations
from pprint import pprint
import json
import os

def load_schema(path: str) -> Schema:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return Schema(schema=data)

if __name__ == "__main__":
    base_path = os.path.join(os.path.dirname(__file__), "..", "examples")
    current_schema = load_schema(os.path.join(base_path, "users_current.json"))
    new_schema = load_schema(os.path.join(base_path, "users_new.json"))

    diff = diff_schemas(current_schema, new_schema)

    print("\nSchema differences:")
    print(diff.pretty())


    operations = generate_evolution_operations(diff)
    print("\nEvolution operations:")
    for op in operations:
        print(op.pretty())
