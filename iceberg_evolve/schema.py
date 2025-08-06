"""
Schema utilities for loading and normalizing JSON-style schemas.

PyIceberg documentation: https://py.iceberg.apache.org/configuration/

Examples:
    # Load from local JSON file
    schema = Schema.from_json_file("schemas/my_table.json")

    # Load from an Iceberg table using a configured catalog (via pyiceberg.yaml)
    schema = Schema.from_iceberg("mydb.mytable", catalog="glue")

    # Load from Iceberg with an explicit REST catalog config
    schema = Schema.from_iceberg(
        "mydb.mytable",
        catalog="rest",
        config={
            "type": "rest",
            "uri": "https://catalog.mycompany.com/api",
            "credential": "token:abc123"
        }
    )

    # Load from SQL catalog (PostgreSQL)
    schema = Schema.from_iceberg(
        "mydb.mytable",
        catalog="sql",
        config={
            "type": "sql",
            "uri": "postgresql+psycopg2://user:pass@host:5432/dbname"
        }
    )

    # Load from Hive
    schema = Schema.from_iceberg(
        "mydb.mytable",
        catalog="hive",
        config={
            "type": "hive",
            "uri": "thrift://localhost:9083"
        }
    )

    # Load from Glue catalog
    schema = Schema.from_iceberg(
        "mydb.mytable",
        catalog="glue",
        config={
            "type": "glue",
            "region": "eu-west-2"
        }
    )
"""
import json

from rich.console import Console

from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.table import Table

from iceberg_evolve.diff import SchemaDiff
from iceberg_evolve.migrate import (
    MoveColumn,
    RenameColumn,
    UnionSchema
)
from iceberg_evolve.exceptions import CatalogLoadError, SchemaParseError
from iceberg_evolve.serializer import IcebergSchemaJSONSerializer
from iceberg_evolve.utils import open_update_context


class Schema:
    """
    Wrapper for a PyIceberg Schema object.
    """
    def __init__(self, iceberg_schema: IcebergSchema):
        self.iceberg_schema = iceberg_schema

    @property
    def fields(self):
        """
        Return the list of NestedField objects in this schema.
        """
        return self.iceberg_schema.fields

    @property
    def schema(self) -> IcebergSchema:
        """
        Return the underlying PyIceberg Schema object.
        """
        return self.iceberg_schema

    def __repr__(self):
        return f"IcebergSchema({self.iceberg_schema})"

    @classmethod
    def from_file(cls, path: str) -> "Schema":
        """
        Load schema from a JSON file. Raises SchemaParseError on failure.
        """
        if not path.lower().endswith(".json"):
            raise ValueError("Currently, only JSON files are supported for schema loading.")
        try:
            with open(path) as f:
                data = json.load(f)
            iceberg_schema = IcebergSchemaJSONSerializer.from_dict(data)
            return cls(iceberg_schema)
        # This block handles JSON parsing failures, such as invalid syntax or schema structure.
        except Exception as e:
            raise SchemaParseError(f"Failed to parse schema from {path}: {e}", path=str(path)) from e

    @classmethod
    def from_iceberg(cls, table_name: str, catalog: str = "glue", config: dict = None) -> "Schema":
        """
        Load schema from an Iceberg table in a catalog using PyIceberg.
        Raises CatalogLoadError on connection or lookup failure.
        """
        try:
            catalog_kwargs = config or {}
            catalog_client = load_catalog(catalog, **catalog_kwargs)
            table = catalog_client.load_table(table_name)
            iceberg_schema = table.schema()
            return cls(iceberg_schema)
        except Exception as e:
            raise CatalogLoadError(
                f"Failed to load table '{table_name}' from catalog '{catalog}': {e}",
                table=table_name,
                catalog=catalog
            ) from e

    @classmethod
    def from_s3(cls, bucket: str, key: str) -> "Schema":
        """
        Load schema from an S3 bucket.
        """
        if not key.lower().endswith(".json"):
            raise ValueError("Currently, only JSON files are supported for schema loading from S3.")
        try:
            import boto3
            s3 = boto3.resource("s3")
            obj = s3.Object(bucket, key)
            data = json.loads(obj.get()["Body"].read().decode("utf-8"))
            iceberg_schema = IcebergSchemaJSONSerializer.from_dict(data)
            return cls(iceberg_schema)
        except Exception as e:
            raise SchemaParseError(
                f"Failed to load schema from S3 s3://{bucket}/{key}: {e}",
                path=f"s3://{bucket}/{key}"
            ) from e

    def evolve(
        self,
        new: "Schema",
        table: Table,
        dry_run: bool = False,
        quiet: bool = False,
        strict: bool = True,
        allow_breaking: bool = False,
        return_applied_schema: bool = False,
        console: Console | None = None,
    ) -> "Schema":
        """
        Evolve this table's schema to match the given new schema by computing and applying
        the minimal set of evolution operations.

        Args:
            new (Schema): The target schema.
            table (Table): The Iceberg table to apply the evolution to.
            dry_run (bool): If True, display changes but do not apply them.
            quiet (bool): If True, suppress output to the console.
            strict (bool): If True, the evolution will fail if any unsupported operations are detected.
            allow_breaking (bool): If True, force updates even if they are breaking changes,
                e.g. dropping a column with data or updating a column with a narrower type.
            return_diff (bool): If True, return a tuple (evolved_schema, diff).
            return_applied_schema (bool): If True, return the evolved schema after applying changes.
                This requires fetching the schema again from the table.
            console (Console | None): Rich console for formatted output.

        Returns:
            Schema: The evolved schema, read from the catalog after applying changes.
        """
        if not isinstance(new, Schema):
            raise ValueError("The 'new' parameter must be an instance of Schema.")
        if not isinstance(table, Table):
            raise ValueError("The 'table' parameter must be an instance of pyiceberg.table.Table.")

        from iceberg_evolve.migrate import UpdateColumn

        console = console or Console()

        diff = SchemaDiff.from_schemas(self, new)
        ops = diff.to_evolution_operations()

        # Check for UnionSchema operations and explicitly reject them if present
        for op in ops:
            if isinstance(op, UnionSchema):
                raise NotImplementedError("UnionSchema operation is not supported in evolve().")

        # Display the diff and operations if not in quiet mode
        if not quiet or dry_run:
            console.print("\n[bold]Schema Evolution Diff:[/bold]\n")
            diff.display(console)
            console.print("\n[bold]Evolution Operations:[/bold]\n")
            for op in ops:
                op.display(console)

        if dry_run:
            console.print("[bold]Dry Run - No Changes Applied[/bold]")
            return self

        allowed_ops = [op for op in ops if not op.is_breaking() or allow_breaking]
        breaking_ops = [op for op in ops if op.is_breaking() and not allow_breaking]

        # 0) Fail on any evolution operation that isn't supported, unless strict is False
        if strict:
            unsupported_ops = [op for op in ops if not getattr(op, "is_supported", True)]
            if unsupported_ops:
                console.print("[bold red]Error:[/bold red] The following evolution operations are unsupported:")
                for op in unsupported_ops:
                    console.print(op.pretty())
                raise RuntimeError(
                    "Aborting schema evolution: one or more operations are not supported."
                )

        # 1) Filter breaking operations if not allowed
        if not allow_breaking and breaking_ops:
            console.print("[bold red]Breaking changes detected but 'allow_breaking' is False:[/bold red]")
            for op in breaking_ops:
                op.display(console)
            raise ValueError("Breaking changes are not allowed unless 'allow_breaking=True'.")

        # Apply the evolution operations in three phases
        phase1 = [op for op in allowed_ops if isinstance(op, RenameColumn)]
        phase3 = [op for op in allowed_ops if isinstance(op, MoveColumn)]
        phase2 = [op for op in allowed_ops if op not in phase1 and op not in phase3]

        # Apply all renames first
        console.print("[bold]Applying Renames...[/bold]")
        if not quiet:
            for op in phase1:
                op.display(console)

        with open_update_context(table, allow_breaking) as update:
            for op in phase1:
                op.apply(update)

        # Re-fetch so renames/adds/etc are visible
        table = table.catalog.load_table(table.name())

        # Apply adds, updates, and drops next
        console.print("[bold]Applying Adds, Updates, and Drops...[/bold]")
        if not quiet:
            for op in phase2:
                op.display(console)

        with open_update_context(table, allow_breaking) as update:
            for op in phase2:
                op.apply(update)

        # Re-fetch again to ensure the schema is up-to-date
        table = table.catalog.load_table(table.name())

        # Apply moves last
        console.print("[bold]Applying Moves...[/bold]")
        if not quiet:
            for op in phase3:
                op.display(console)

        with open_update_context(table, allow_breaking) as update:
            for op in phase3:
                op.apply(update)

        console.print("[bold]Schema Evolution Applied Successfully![/bold]")

        result = self
        if return_applied_schema:
            console.print("[bold]Fetching Evolved Schema from Table.[/bold]")
            catalog = table.catalog
            new_table = catalog.load_table(table.name())
            result = Schema(new_table.schema())

        return result
