import pytest

from iceberg_evolve.migrate import AddColumn, MoveColumn, RenameColumn, UnionSchema
from iceberg_evolve.schema import Schema as EvolveSchema
from iceberg_evolve.serializer import IcebergSchemaJSONSerializer
import iceberg_evolve.schema as sc_module
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.types import NestedField, StringType


def test_repr_returns_string():
    """Test that the __repr__ method returns a string representation of the schema."""
    schema_dict = {
        "schema-id": 0,
        "fields": [{"id": 1, "name": "id", "type": "int", "required": True}]
    }
    iceberg_schema = IcebergSchemaJSONSerializer.from_dict(schema_dict)
    schema = EvolveSchema(iceberg_schema)
    assert isinstance(repr(schema), str)


def test_from_file_reads_schema(tmp_path):
    """Test loading a schema from a local JSON file."""
    path = tmp_path / "schema.json"
    path.write_text('{"schema-id": 0, "fields": [{"id": 1, "name": "id", "type": "int", "required": true}]}')
    schema = EvolveSchema.from_file(str(path))
    assert [f.name for f in schema.fields] == ["id"]


def test_from_file_invalid_extension():
    """Test that loading a schema from a file with an invalid extension raises a ValueError."""
    with pytest.raises(ValueError, match="Currently, only JSON files are supported for schema loading."):
        EvolveSchema.from_file("schema.txt")


def test_from_file_invalid_json(tmp_path):
    """Test that loading a schema with invalid JSON content raises SchemaParseError."""
    path = tmp_path / "invalid_schema.json"
    path.write_text('{"schema-id": 0, "fields": [')  # Incomplete JSON
    from iceberg_evolve.exceptions import SchemaParseError
    with pytest.raises(SchemaParseError, match="Failed to parse schema from"):
        EvolveSchema.from_file(str(path))


def test_from_s3_reads_schema(monkeypatch):
    """Test loading a schema from an S3 bucket using a mocked boto3 client."""
    class MockS3Object:
        def get(self):
            return {"Body": MockBody()}

    class MockBody:
        def read(self):
            return b'{"schema-id": 0, "fields": [{"id": 1, "name": "id", "type": "int", "required": true}]}'

    class MockS3Resource:
        def Object(self, bucket, key):
            return MockS3Object()

    import sys
    mock_boto3 = type("boto3", (), {"resource": lambda x: MockS3Resource()})
    monkeypatch.setitem(sys.modules, "boto3", mock_boto3)

    schema = EvolveSchema.from_s3("bucket", "schema.json")
    assert [f.name for f in schema.fields] == ["id"]


def test_from_s3_raises_schema_parse_error(monkeypatch):
    """Test that Schema.from_s3 raises SchemaParseError on S3 failures."""
    class MockFailingObject:
        def get(self):
            raise RuntimeError("Simulated S3 failure")

    class MockS3Resource:
        def Object(self, bucket, key):
            return MockFailingObject()

    import sys
    mock_boto3 = type("boto3", (), {"resource": lambda x: MockS3Resource()})
    monkeypatch.setitem(sys.modules, "boto3", mock_boto3)

    from iceberg_evolve.exceptions import SchemaParseError
    with pytest.raises(SchemaParseError, match="Failed to load schema from S3 s3://bucket/key.json: Simulated S3 failure"):
        EvolveSchema.from_s3("bucket", "key.json")


def test_from_s3_invalid_extension():
    """Test that loading a schema from S3 with an invalid file extension raises a ValueError."""
    with pytest.raises(ValueError, match="Currently, only JSON files are supported for schema loading from S3."):
        EvolveSchema.from_s3("bucket", "schema.txt")


def test_from_iceberg_uses_loader(monkeypatch):
    """Test loading a schema from an Iceberg table using a mocked catalog loader."""
    mock_schema = IcebergSchema(
        NestedField(field_id=1, name="id", field_type=StringType(), required=True)
    )

    class MockCatalog:
        def load_table(self, table_name):
            class MockTable:
                def schema(self):  # must be a method to match pyiceberg.Table interface
                    return mock_schema
            return MockTable()

    import iceberg_evolve.schema
    monkeypatch.setattr(iceberg_evolve.schema, "load_catalog", lambda name, **kwargs: MockCatalog())

    schema = EvolveSchema.from_iceberg("table", "glue")
    assert [f.name for f in schema.fields] == ["id"]


def test_from_iceberg_raises_catalog_error(monkeypatch):
    """Test that Schema.from_iceberg raises CatalogLoadError on failure."""
    class MockFailingCatalog:
        def load_table(self, table_name):
            raise RuntimeError("Simulated failure")

    import iceberg_evolve.schema
    monkeypatch.setattr(iceberg_evolve.schema, "load_catalog", lambda name, **kwargs: MockFailingCatalog())

    from iceberg_evolve.exceptions import CatalogLoadError
    with pytest.raises(CatalogLoadError, match="Failed to load table 'table' from catalog 'glue': Simulated failure"):
        EvolveSchema.from_iceberg("table", "glue")


def test_schema_property_returns_iceberg_schema():
    """Test that the 'schema' property returns the underlying Iceberg schema."""
    iceberg_schema = IcebergSchema(
        NestedField(field_id=1, name="id", field_type=StringType(), required=True)
    )
    schema = EvolveSchema(iceberg_schema)
    assert schema.schema == iceberg_schema


class DummyTable:
    def __init__(self):
        self.catalog = self
        # For catalog.load_table
    def name(self):
        return "dummy_name"
    def load_table(self, identifier):
        return self
    def identifier(self):
        return "dummy"
    def schema(self):
        # Return a trivial schema
        return IcebergSchema(NestedField(field_id=1, name="id", field_type=StringType(), required=True))
    def update_schema(self):
        class Ctx:
            def __enter__(inner):
                # Provide a dummy update object
                class Update:
                    def __init__(self):
                        self.applied = []
                return Update()
            def __exit__(inner, exc_type, exc, tb):
                return False
        return Ctx()


def test_evolve_invalid_new_arg():
    """Evolve should reject a non-Schema 'new' argument."""
    original = EvolveSchema(IcebergSchema(NestedField(1, "id", StringType(), required=True)))
    # Monkeypatch Table to DummyTable so table check passes
    sc_module.Table = DummyTable
    with pytest.raises(ValueError, match="must be an instance of Schema"):
        original.evolve(new="not_a_schema", table=DummyTable())


def test_evolve_invalid_table_arg():
    """Evolve should reject a non-Table 'table' argument."""
    original = EvolveSchema(IcebergSchema(NestedField(1, "id", StringType(), required=True)))
    # Ensure Table is unmodified so object() fails isinstance
    # (or reassign Table to DummyTable so isinstance fails for object())
    sc_module.Table = DummyTable
    with pytest.raises(ValueError, match="must be an instance of pyiceberg.table.Table"):
        original.evolve(new=original, table=object())


def test_evolve_not_supported_union(monkeypatch):
    """Evolve should reject UnionSchema operations."""
    original = EvolveSchema(IcebergSchema(NestedField(1, "id", StringType(), required=True)))
    sc_module.Table = DummyTable
    # Stub SchemaDiff.from_schemas to return a diff with a UnionSchema op
    class DummyDiff:
        def to_evolution_operations(self):
            return [UnionSchema(name="x", new_type=IcebergSchema())]
    monkeypatch.setattr(
        sc_module.SchemaDiff,
        "from_schemas",
        classmethod(lambda cls, old, new: DummyDiff())
    )
    with pytest.raises(NotImplementedError):
        original.evolve(new=original, table=DummyTable())


def test_evolve_dry_run_no_apply(monkeypatch):
    """Dry run should not invoke update_schema and return original schema."""
    original = EvolveSchema(IcebergSchema(NestedField(1, "id", StringType(), required=True)))
    sc_module.Table = DummyTable
    # Create a diff with one AddColumn op
    class DummyDiff:
        def to_evolution_operations(self):
            return [AddColumn(name="col1", new_type=StringType())]
        def display(self, console):
            pass
    monkeypatch.setattr(
        sc_module.SchemaDiff,
        "from_schemas",
        classmethod(lambda cls, old, new: DummyDiff())
    )
    result = original.evolve(new=original, table=DummyTable(), dry_run=True)
    assert result is original


def test_evolve_breaking_not_allowed(monkeypatch):
    """Evolve should raise on breaking operations when not allowed."""
    original = EvolveSchema(IcebergSchema(NestedField(1, "id", StringType(), required=True)))
    sc_module.Table = DummyTable
    # Dummy op that is breaking
    class BrkOp:
        def is_breaking(self): return True
        def display(self, console): pass
    class DummyDiff:
        def to_evolution_operations(self): return [BrkOp()]
        def display(self, console): pass
    monkeypatch.setattr(
        sc_module.SchemaDiff,
        "from_schemas",
        classmethod(lambda cls, old, new: DummyDiff())
    )
    with pytest.raises(ValueError, match="Breaking changes are not allowed"):
        original.evolve(new=original, table=DummyTable())


def test_evolve_apply_ops(monkeypatch):
    """Evolve should apply non-breaking and breaking ops when allowed."""
    original = EvolveSchema(IcebergSchema(NestedField(1, "id", StringType(), required=True)))
    sc_module.Table = DummyTable
    # Dummy ops that record names when applied
    class Op:
        def __init__(self, name, breaking):
            self.name = name
            self._breaking = breaking
        def is_breaking(self): return self._breaking
        def display(self, console): pass
        def apply(self, update):
            update.applied.append(self.name)
    class DummyDiff:
        def to_evolution_operations(self):
            return [Op("a", False), Op("b", True)]
        def display(self, console): pass
    monkeypatch.setattr(
        sc_module.SchemaDiff,
        "from_schemas",
        classmethod(lambda cls, old, new: DummyDiff())
    )
    # Call evolve allowing breaking changes
    instance_table = DummyTable()
    result = original.evolve(new=original, table=instance_table, allow_breaking=True)
    # Verify that both ops were applied
    # Access the update context's applied list
    # Since DummyTable.update_schema returns a new context each time, patch to capture it
    # So re-invoke but capture update via a closure
    applied = []
    class CaptureTable(DummyTable):
        def update_schema(self):
            class Ctx:
                def __enter__(inner):
                    class Update:
                        def __init__(self, applied_list):
                            self.applied = applied_list
                    return Update(applied)
                def __exit__(inner, exc_type, exc, tb):
                    return False
            return Ctx()
    sc_module.Table = CaptureTable
    instance_table = CaptureTable()
    original.evolve(new=original, table=instance_table, allow_breaking=True)
    assert applied == ["a", "b"]


def test_evolve_return_applied_schema(monkeypatch):
    """Evolve should return the applied schema when requested."""
    # Original and new schemas
    orig_schema = EvolveSchema(IcebergSchema(NestedField(1, "id", StringType(), required=True)))
    new_iceberg = IcebergSchema(NestedField(1, "id", StringType(), required=True))
    # Dummy table that returns new schema on fetch
    class FetchTable(DummyTable):
        def update_schema(self):
            # no-op context
            return super().update_schema()
        def identifier(self):
            return "dummy"
    fetch_table = FetchTable()
    fetch_table.catalog = fetch_table
    monkeypatch.setattr(
        sc_module.SchemaDiff,
        "from_schemas",
        classmethod(lambda cls, old, new: type("D", (), {
            "to_evolution_operations": lambda self: [],
            "display": lambda self, console: None
        })())
    )
    # Monkeypatch load_table on catalog to return the same FetchTable (so it still has update_schema)
    fetch_table.load_table = lambda ident: fetch_table

    sc_module.Table = FetchTable
    result = orig_schema.evolve(new=orig_schema, table=fetch_table, return_applied_schema=True)
    assert isinstance(result, EvolveSchema)
    assert result.schema == new_iceberg


def test_evolve_phases_display_and_apply(monkeypatch):
    """
    Evolve should:
      - Print each phase header and call op.display when quiet=False.
      - Invoke op.apply(update) under each open_update_context.
    """

    # 1) Prepare original and new (they're the same, since diff is stubbed)
    sdk = IcebergSchema(NestedField(1, "id", StringType(), required=True))
    original = EvolveSchema(sdk)
    sc_module.Table = None  # make sure isinstance(table, Table) passes; we'll override below

    # 2) Build dummy ops: one rename, one add, one move
    rename_op = RenameColumn(name="old_name", target="new_name")
    add_op    = AddColumn(name="foo", new_type=StringType())
    move_op   = MoveColumn(name="bar", target="", position="first")

    # Stub out SchemaDiff.from_schemas -> dummy diff
    class DummyDiff:
        def to_evolution_operations(self):
            return [rename_op, add_op, move_op]
        def display(self, console):
            pass  # we won't test the diff.display itself here

    monkeypatch.setattr(
        sc_module.SchemaDiff,
        "from_schemas",
        classmethod(lambda cls, old, new: DummyDiff())
    )

    # 3) Prepare a dummy console to capture prints
    printed = []
    class DummyConsole:
        def print(self, msg=None):
            printed.append(msg)

    # 4) CaptureTable: instrument rename_column, add_column, move_first
    applied = []
    class CaptureTable:
        # mimic pyiceberg.table.Table
        def __init__(self):
            self.catalog = self
        def name(self): return "dummy"
        def load_table(self, _): return self

        def update_schema(self):
            class Ctx:
                def __enter__(inner):
                    class Update:
                        def rename_column(self, name, target):
                            applied.append(f"rename:{name}->{target}")
                        def add_column(self, name, new_type, doc=None):
                            applied.append(f"add:{name}")
                        def move_first(self, name):
                            applied.append(f"move_first:{name}")
                    return Update()
                def __exit__(inner, exc_type, exc, tb):
                    return False
            return Ctx()

    sc_module.Table = CaptureTable
    table = CaptureTable()

    # 5) Call evolve with quiet=False (default)
    result = original.evolve(
        new=original,
        table=table,
        allow_breaking=True,
        quiet=False,
        console=DummyConsole()
    )
    # Evolve returns self when return_applied_schema=False
    assert result is original

    # 6) Assertions:

    # a) Phase headers printed in order:
    #    - "[bold]Applying Renames...[/bold]"
    #    - "[bold]Applying Adds, Updates, and Drops...[/bold]"
    #    - "[bold]Applying Moves...[/bold]"
    bodies = [str(x) for x in printed if isinstance(x, str)]
    assert any("Applying Renames" in b for b in bodies),    "should print rename header"
    assert any("Applying Adds, Updates" in b for b in bodies), "should print add/update header"
    assert any("Applying Moves" in b for b in bodies),     "should print move header"

    # b) Each op.display(console) was invoked once:
    #    (RenameColumn.pretty / AddColumn.pretty / MoveColumn.pretty each call console.print internally,
    #     but we're mostly checking that display() was called at all by virtue of no errors)

    # c) Each apply method mapped to the correct Update method call
    assert "rename:old_name->new_name" in applied
    assert "add:foo" in applied
    assert "move_first:bar" in applied


def make_original():
    # helper to create a base Schema instance
    iceberg = IcebergSchema(NestedField(1, "id", StringType(), required=True))
    return EvolveSchema(iceberg)

def test_strict_default_fails_on_unsupported(monkeypatch):
    """
    When strict=True, any op with is_supported=False should cause evolve() to
    print an error and raise RuntimeError.
    """
    original = make_original()
    sc_module.Table = DummyTable

    # Dummy op that's unsupported
    class UnsupportedOp:
        is_breaking = lambda self: False
        is_supported = False
        def pretty(self):
            return "<UNSUPPORTED>"
        def display(self, console):
            console.print("displayed")
        def apply(self, update):
            pass

    # Stub the diff to return our single unsupported op
    class DummyDiff:
        def to_evolution_operations(self):
            return [UnsupportedOp()]
        def display(self, console):
            pass

    monkeypatch.setattr(
        sc_module.SchemaDiff,
        "from_schemas",
        classmethod(lambda cls, old, new: DummyDiff())
    )

    # Capture console output
    printed = []
    class DummyConsole:
        def print(self, msg=None):
            printed.append(msg)

    with pytest.raises(RuntimeError, match="Aborting schema evolution"):
        original.evolve(
            new=original,
            table=DummyTable(),
            strict=True,       # exercise the failure path
            quiet=True,        # skip diff/ops prints
            console=DummyConsole()
        )

    # First print should be the Error header, second the op.pretty()
    assert printed[0].startswith("[bold red]Error"), "Expected error header"
    assert "<UNSUPPORTED>" in printed[1], "Expected pretty() output of unsupported op"

def test_non_strict_allows_unsupported(monkeypatch):
    """
    When strict=False, unsupported ops should be skipped (no RuntimeError),
    and evolve() should return normally.
    """
    original = make_original()
    sc_module.Table = DummyTable

    # Same dummy op as above
    class UnsupportedOp:
        is_breaking = lambda self: False
        is_supported = False
        def pretty(self): return "<UNSUPPORTED>"
        def apply(self, update): pass

    class DummyDiff:
        def to_evolution_operations(self):
            return [UnsupportedOp()]
        def display(self, console):
            pass

    monkeypatch.setattr(
        sc_module.SchemaDiff,
        "from_schemas",
        classmethod(lambda cls, old, new: DummyDiff())
    )

    # Use a dummy console to verify we still see the warning
    printed = []
    class DummyConsole:
        def print(self, msg=None):
            printed.append(msg)

    result = original.evolve(
        new=original,
        table=DummyTable(),
        strict=False,     # do not fail on unsupported
        quiet=True,
        console=DummyConsole()
    )

    # Should simply return self
    assert result is original
    # It should *not* print the Error header when strict=False
    assert not any("Error:" in str(p) for p in printed), (
        f"Did not expect an error header when strict=False, but got: {printed}"
    )


def test_strict_fails_on_any_unsupported(monkeypatch):
    """
    If strict=True, *any* op with is_supported=False should trigger the fail-fast block:
    1) print the Error header,
    2) print each op.pretty(),
    3) raise RuntimeError.
    """
    from iceberg_evolve.schema import Schema as EvolveSchema
    import iceberg_evolve.schema as sc_mod
    from pyiceberg.schema import Schema as IcebergSchema
    from pyiceberg.types import NestedField, StringType

    # Build a dummy Schema and table
    base = EvolveSchema(IcebergSchema(NestedField(1, "id", StringType(), required=True)))
    class DummyTable:
        def name(self): return "dummy"
        def load_table(self, _): return self
        def update_schema(self):
            class Ctx:
                def __enter__(self): return self
                def __exit__(self, *_): return False
            return Ctx()

    # allow DummyTable to pass isinstance(table, Table)
    monkeypatch.setattr(sc_mod, "Table", DummyTable)

    # Monkey-patch SchemaDiff to yield one unsupported op
    class Op:
        is_supported = False
        def pretty(self): return "UNSUPPORTED-OP"
        def display(self, console): pass
        def is_breaking(self): return False

    class DummyDiff:
        def to_evolution_operations(self): return [Op()]
        def display(self, console): pass

    monkeypatch.setattr(
        sc_mod.SchemaDiff,
        "from_schemas",
        classmethod(lambda cls, old, new: DummyDiff())
    )

    # Capture console prints
    printed = []
    class C:
        def print(self, msg=None):
            printed.append(msg)

    with pytest.raises(RuntimeError, match="one or more operations are not supported"):
        base.evolve(
            new=base,
            table=DummyTable(),
            strict=True,
            quiet=True,
            console=C()
        )

    # Validate output
    assert any("Error:" in str(line) for line in printed), "Expected Error header"
    assert any("UNSUPPORTED-OP" in str(line) for line in printed), "Expected op.pretty() output"
