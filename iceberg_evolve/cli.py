import json
import typer

from iceberg_evolve.diff import SchemaDiff
from iceberg_evolve.schema import Schema
from iceberg_evolve.serializer import IcebergSchemaJSONSerializer

app = typer.Typer(help="Iceberg-Evolve command-line interface")

@app.command()
def diff(
    old_schema: typer.FileText,
    new_schema: typer.FileText,
    match_by: str = typer.Option(
        "id",
        "--match-by",
        help="Matching strategy: name or id.",
    ),
    json_output: bool = typer.Option(
        False,
        "--json",
        help="Output operations as JSON. If not set, displays human-friendly format.",
    ),
) -> None:
    """
    Show schema difference operations between two Iceberg schemas.

    Example 1: Compare by name and print human-friendly operations

    $ iceberg-evolve diff old_schema.json new_schema.json

    Example 2: Compare by id and output JSON

    $ iceberg-evolve diff old_schema.json new_schema.json --match-by id --json
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
            typer.echo(op.display(), nl=False)


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
        False, "--return-applied-schema", help="Return the applied schema after evolution and prints it to console in JSON format."
    )
) -> None:
    """
    Apply a schema evolution to the specified Iceberg table.

    Example:

    $ iceberg-evolve evolve -c hive://localhost:9083 -t default.users -p new_schema.json
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
        from iceberg_evolve.serializer import IcebergSchemaJSONSerializer

        schema_dict = IcebergSchemaJSONSerializer.to_dict(applied.schema)
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


def _parse_json_config(
    ctx: typer.Context,
    param: typer.CallbackParam,
    value: str | None,
) -> dict | None:
    if not value:
        return None
    try:
        return json.loads(value)
    except json.JSONDecodeError as e:
        raise typer.BadParameter(f"Invalid JSON for --{param.name}: {e}")


@app.command()
def serialize(
    catalog_url: str = typer.Option(
        ..., "--catalog-url", "-c",
        help="Catalog URL or alias (e.g. hive://localhost:9083 or a name from pyiceberg.yaml)."
    ),
    table_ident: str = typer.Option(
        ..., "--table-ident", "-t",
        help="Iceberg table identifier, e.g. 'db.schema.table' or 'namespace.table'."
    ),
    output_path: str = typer.Option(
        ..., "--output-path", "-p",
        help="Filesystem path where the standalone JSON schema will be written."
    ),
    config: str | None = typer.Option(
        None,
        "--config",
        callback=_parse_json_config,
        help=(
            "Optional catalog configuration as a JSON string. "
            "E.g. '{\"warehouse\":\"/data/iceberg\",\"hive-site.xml\":\"/etc/hive/conf/hive-site.xml\"}'"
        )
    )
):
    """
    Serialize an Iceberg table's schema to a standalone JSON file.
    """
    # Pass a `config` argument (None) so tests expecting three params bind correctly
    schema_obj = Schema.from_iceberg(table_name=table_ident, catalog=catalog_url, config=config)

    # Turn it into the Iceberg JSON‐schema dict
    schema_dict = IcebergSchemaJSONSerializer.to_dict(schema_obj.schema)

    # Write out to disk
    with open(output_path, "w") as f:
        json.dump(schema_dict, f, indent=2)

    typer.secho(f"✅ Schema for '{table_ident}' written to {output_path}", fg=typer.colors.GREEN)


if __name__ == "__main__":
    app()
