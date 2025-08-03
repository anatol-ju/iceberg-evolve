import sys
import json
import typer
from iceberg_evolve.diff import SchemaDiff
from iceberg_evolve.schema import Schema

app = typer.Typer(help="Iceberg-Evolve command-line interface")

@app.command()
def diff(
    old_schema: typer.FileText,
    new_schema: typer.FileText,
    match_by: str = typer.Option(
        "id",
        "--match-by",
        help="Matching strategy: name or id. Defaults to 'id' (Iceberg standard).",
    ),
    json_output: bool = typer.Option(
        False,
        "--json",
        help="Output operations as JSON. If not set, displays human-friendly format.",
    ),
) -> None:
    """
    Show schema difference operations between two Iceberg schemas.

    Args:
        old_schema: Path to the old schema JSON file or a JSON string.
        new_schema: Path to the new schema JSON file or a JSON string.
        match_by: How to match fields between schemas, either by 'name' or 'id'.
        json_output: If true, outputs operations as JSON; otherwise, displays human-friendly format.

    Example:
    ```
    # Compare by name and print human-friendly operations
    iceberg-evolve diff old_schema.json new_schema.json

    # Compare by id and output raw JSON
    iceberg-evolve diff old_schema.json new_schema.json --match-by id --json
    ```
    """
    # Load schemas
    old = (
        Schema.from_file(old_schema.name)
        if hasattr(Schema, "from_file")
        else Schema(json.load(old_schema))
    )
    new = (
        Schema.from_file(new_schema.name)
        if hasattr(Schema, "from_file")
        else Schema(json.load(new_schema))
    )

    # Compute diff
    if match_by == "name":
        diff_obj = SchemaDiff.union_by_name(old, new)
    else:
        # Build a diff matching by ID, the Iceberg standard
        diff_obj = SchemaDiff.from_schemas(old, new)

    ops = diff_obj.to_evolution_operations()

    if json_output:
        # Serialize operations to JSON
        serialized = [op.to_dict() for op in ops]
        typer.echo(json.dumps(serialized, indent=2))
    else:
        # Human-friendly display
        for op in ops:
            typer.echo(op.display())

@app.command()
def evolve(
    catalog_url: str = typer.Option(
        ..., "--catalog-url", "-c", help="Catalog URI, e.g. 'hive://...'."
    ),
    table_ident: str = typer.Option(
        ..., "--table-ident", "-t", help="Table identifier, e.g. 'db.table'."
    ),
    schema_path: typer.FileText = typer.Option(
        ..., "--schema-path", "-p", help="Path to new schema JSON file."
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Preview changes without applying them."
    ),
    quiet: bool = typer.Option(
        False, "--quiet", help="Suppress output messages."
    ),
    strict: bool = typer.Option(
        True,
        "--strict/--no-strict",
        help="Fail on unsupported evolution operations (default). Use --no-strict to skip them and apply supported ones.",
    ),
    allow_breaking: bool = typer.Option(
        False, "--allow-breaking", help="Allow breaking changes in schema evolution."
    ),
    return_applied_schema: bool = typer.Option(
        False, "--return-applied-schema", help="Return the applied schema after evolution."
    )
) -> None:
    """
    Apply a schema evolution to the specified Iceberg table.

    Args:
        catalog_url: URI of the Iceberg catalog (e.g., 'hive://localhost:9083').
        table_ident: Identifier of the Iceberg table (e.g., 'default.users').
        schema_path: Path to the new schema JSON file.
        dry_run: If true, shows what changes would be made without applying them.
        quiet: If true, suppresses output messages.
        allow_breaking: If true, allows breaking changes in the schema evolution.
        return_applied_schema: If true, returns the applied schema after evolution.

    Example:
    ```
    iceberg-evolve evolve -c hive://localhost:9083 -t default.users -p new_schema.json
    ```
    """
    # Load new schema
    new = Schema.from_file(schema_path.name)

    # Connect to table and current schema
    from pyiceberg.catalog import load_catalog

    catalog = load_catalog(catalog_url)
    tbl = catalog.load_table(table_ident)
    current = Schema(tbl.schema())

    applied = current.evolve(
        new=new,
        table=tbl,
        dry_run=dry_run,
        quiet=quiet,
        strict=strict,
        allow_breaking=allow_breaking,
        return_applied_schema=return_applied_schema
    )

    if return_applied_schema:
        # Serialize the applied (evolved) schema back into JSON for human consumption
        from iceberg_evolve.utils import IcebergSchemaSerializer

        schema_dict = IcebergSchemaSerializer.to_dict(applied.schema)
        # Print the schema in a human-readable format
        if not quiet:
            typer.secho("Evolved schema:", fg=typer.colors.GREEN)
            typer.echo("```json")
            typer.echo(json.dumps(schema_dict, indent=2))
            typer.echo("```")
        return
    else:
        if not quiet:
            typer.secho("Schema evolution operations applied successfully.", fg=typer.colors.GREEN)

    typer.secho("Schema evolution complete", fg=typer.colors.GREEN)

if __name__ == "__main__":
    app()
