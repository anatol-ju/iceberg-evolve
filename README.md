# iceberg-evolve
Schema diffing and evolution tool for Apache Iceberg and beyond.

## 📣 New in 1.0.0
Initial release with core support for schema comparison and automated evolution against live Iceberg tables.

## 🔧 Features
- Schema Loading
  - Store and load Iceberg schemas to/from standalone JSON files via IcebergSchemaJSONSerializer.
  - Fetch table schemas directly from Iceberg catalogs (Hive, Glue, REST) via PyIceberg configurations (pyiceberg.yaml).

- Schema Diffing
  - Detect added, removed, renamed, and type-changed columns.
  - Support matching by column id or name strategies (default: id).

- Automated Evolution
  - Generate and apply Iceberg schema evolution operations (add/rename/update/drop).
  - Preview migrations with a `--dry-run` mode before applying changes.

- Rich CLI
  - `iceberg-evolve diff <old.json> <new.json>` to view schema diffs in a colored, tree-style format.
  - `iceberg-evolve evolve --catalog-url <URI> --table-ident <db.table> --schema-path <new.json>` to apply migrations.

- Python API
  - Programmatic access to `Schema`, `SchemaDiff`, and migration utilities for integration in CI/CD pipelines or custom scripts.

- Utilities
  - Clean and normalize Iceberg type strings.
  - Render operation plans to console via [Rich](https://pypi.org/project/rich/).

## 🚀 Use Cases
- Automate schema migrations for data lakes built on Iceberg.
- Integrate schema checks into CI/CD workflows to prevent accidental breaking changes.
- Generate human-readable schema evolution plans for review and auditing.
- Build Python tooling around Iceberg schemas, including advanced analyses and reporting.

## 🚚 Installation
Requires Python 3.10 or later.
```
pip install iceberg-evolve
```

Or, to install for development with Poetry:
```
git clone https://github.com/anatol-ju/iceberg-evolve.git
cd iceberg-evolve
poetry install --with dev
pre-commit install  # optional: enable linting and formatting hooks
```

## 🧱 Quick Examples
For a quick look at the output, install the project and run:
```
poetry run example
```

### Python API
```python
from iceberg_evolve.schema import Schema
from iceberg_evolve.diff import SchemaDiff
from iceberg_evolve.renderer import SchemaDiffRenderer

# Load schemas
old = Schema.from_json_file("schemas/users_current.json")
new = Schema.from_json_file("schemas/users_new.json")

# Compute diff and render to console
diff = SchemaDiff(old, new)
SchemaDiffRenderer(diff).display()

from iceberg_evolve.schema import Schema
from iceberg_evolve.serializer import IcebergSchemaJSONSerializer

# Load an Iceberg Schema from a local file (in the expected format)
old_schema = Schema.from_json_file("schemas/users_current.json")

# Write it out to a standalone JSON file...
IcebergSchemaJSONSerializer.to_json_file(old_schema, "schemas/users_exported.json")

# ...and read it back in later
reloaded_schema = IcebergSchemaJSONSerializer.from_json_file("schemas/users_exported.json")
```

### CLI
```
# View diff between two JSON schemas
iceberg-evolve diff users_current.json users_new.json \
  --match-by name

# Apply evolution to a live Iceberg table (dry run)
iceberg-evolve evolve \
  --catalog-url hive://localhost:9083 \
  --table-ident analytics.users \
  --schema-path users_new.json \
  --dry-run

# Serialize a table's schema
iceberg-evolve serialize \
  --catalog-url hive://localhost:9083 \
  --table-ident analytics.users \
  --output-path schemas/users_table_schema.json
```

## ⚙️ Configuration
This package relies on PyIceberg, therefore the configuration is the same. See [documentation](https://py.iceberg.apache.org/configuration/).
Create a `pyiceberg.yaml` in your project root to configure catalogs:
```yaml
catalogs:
  default:
    type: hive
    uri: thrift://localhost:9083

  glue:
    type: glue
    region: eu-west-1
```
You can find an example configuration in the examples directory. Alternatively, you can use environmental variables to set the catalog details.

When using the CLI, pass the catalog name or full URI to the `evolve` command via `--catalog-url` (e.g., `glue://default`).

## 🧪 Testing
Run unit tests with pytest:
```
poetry run pytest
```
Coverage reports are generated automatically via the existing configuration.

This project contains a basic local setup to test the functionality with a `hive` metastore. The purpose is to give you some insights before applying the package in your pipelines.
You can run integration tests, once the Docker containers are up. Either by:
```
poetry run pytest tests/test_integration.py
```
Or without logging into the container:
```
docker compose exec runner poetry run pytest tests/test_integration.py
```
You don't have to select the integration test explicitly, it will be skipped automatically if you run unit tests outside of a container.

## 📝 License
This project is licensed under the MIT License. See the LICENSE file for details.

## 🧑‍💻 Author
Anatol Jurenkow
Cloud Data Engineer | AWS Enthusiast | Iceberg Fan
[GitHub](https://github.com/anatol-ju) · [LinkedIn](https://www.linkedin.com/in/anatol-jurenkow)

Feel free to open issues or contribute via pull requests—happy evolving!
