import json
import subprocess
import sys
from types import SimpleNamespace

import pytest
from typer.testing import CliRunner

import iceberg_evolve.cli as cli_module
from iceberg_evolve.cli import _parse_json_config

class DummyOp:
    def __init__(self, display_str, to_dict_dict):
        self._display = display_str
        self._dict = to_dict_dict
    def display(self):
        return self._display
    def to_dict(self):
        return self._dict

class DummyDiff:
    def __init__(self, old=None, new=None):
        pass
    @classmethod
    def from_schemas(cls, old, new, *args, **kwargs):
        # ignore any extra params like match_by for tests
        return cls(old, new)
    @classmethod
    def union_by_name(cls, old, new):
        return cls(old, new)
    def to_evolution_operations(self):
        return [DummyOp("OP_HUMAN", {"op":"dummy"}), DummyOp("OP_HUMAN2", {"op":"dummy2"})]

class DummySchema:
    def __init__(self, iceberg_schema=None):
        self.schema = iceberg_schema
    @classmethod
    def from_file(cls, path):
        return cls()
    @classmethod
    def from_iceberg(cls, table_name, catalog, config):
        return cls(iceberg_schema="dummy_schema")
    def evolve(self, new, table, dry_run, quiet, strict, allow_breaking, return_applied_schema):
        # record arguments for testing via attribute
        self._evolve_args = (new, table, dry_run, quiet, strict, allow_breaking, return_applied_schema)
        # return self for chaining
        return self

class DummyTable:
    def schema(self):
        return None

@pytest.fixture(autouse=True)
def patch_cli(monkeypatch):
    # Patch SchemaDiff and Schema in cli module
    monkeypatch.setattr(cli_module, 'SchemaDiff', DummyDiff)
    monkeypatch.setattr(cli_module, 'Schema', DummySchema)
    yield

runner = CliRunner()

def test_diff_human_friendly(tmp_path):
    # Create dummy files (content not used)
    old = tmp_path / "old.json"
    new = tmp_path / "new.json"
    old.write_text('{}')
    new.write_text('{}')

    result = runner.invoke(cli_module.app, ["diff", str(old), str(new)])
    assert result.exit_code == 0
    # Should print both DummyOp.display() outputs
    assert "OP_HUMAN" in result.stdout
    assert "OP_HUMAN2" in result.stdout


def test_diff_json_output(tmp_path):
    old = tmp_path / "a.json"
    new = tmp_path / "b.json"
    old.write_text('{}')
    new.write_text('{}')

    result = runner.invoke(cli_module.app, ["diff", str(old), str(new), "--json"])
    assert result.exit_code == 0
    # Output should be a JSON array matching DummyOp.to_dict()
    data = json.loads(result.stdout)
    assert isinstance(data, list)
    assert data == [{"op":"dummy"}, {"op":"dummy2"}]


def test_diff_match_by_name(tmp_path):
    old = tmp_path / "o.json"
    new = tmp_path / "n.json"
    old.write_text('{}')
    new.write_text('{}')

    # Should call union_by_name path without error
    result = runner.invoke(cli_module.app, ["diff", str(old), str(new), "--match-by", "name"])
    assert result.exit_code == 0
    assert "OP_HUMAN" in result.stdout

@pytest.mark.parametrize("flags, expected_lines", [
    ([], ["Schema evolution operations applied successfully.", "Schema evolution complete"]),
    (["--quiet"], ["Schema evolution complete"]),
])
def test_evolve_default(monkeypatch, tmp_path, flags, expected_lines):
    # Prepare dummy schema file
    schema = tmp_path / "schema.json"
    schema.write_text('{}')
    # Patch load_catalog to return DummyTable loader
    import pyiceberg.catalog as cat_mod
    monkeypatch.setattr(cat_mod, 'load_catalog', lambda url: type('C', (), {'load_table': lambda self, ident: DummyTable()})())

    cmd = ["evolve", "-c", "fake://", "-t", "db.tbl", "-p", str(schema)] + flags
    result = runner.invoke(cli_module.app, cmd)
    assert result.exit_code == 0
    for line in expected_lines:
        assert line in result.stdout


def test_evolve_return_applied_schema(monkeypatch, tmp_path):
    # Prepare schema file
    schema = tmp_path / "s.json"
    schema.write_text('{}')
    # Patch load_catalog
    import pyiceberg.catalog as cat_mod
    monkeypatch.setattr(cat_mod, 'load_catalog', lambda url: type('C', (), {'load_table': lambda self, ident: DummyTable()})())
    # Patch serializer
    import iceberg_evolve.serializer as serializer_mod
    monkeypatch.setattr(serializer_mod.IcebergSchemaJSONSerializer, 'to_dict', lambda s: {"dummy":"schema"})

    result = runner.invoke(cli_module.app, ["evolve", "-c", "u", "-t", "t", "-p", str(schema), "--return-applied-schema"])
    assert result.exit_code == 0
    # Should include header and JSON block
    assert "Evolved schema:" in result.stdout
    assert '"dummy": "schema"' in result.stdout


def test_cli_main_module_diff_json(tmp_path):
    # 1. Write two minimal schema files
    old = tmp_path / "old.json"
    new = tmp_path / "new.json"
    old.write_text(json.dumps({"fields": []}))
    new.write_text(json.dumps({"fields": []}))

    # 2. Invoke the CLI as a module: python -m iceberg_evolve.cli diff ... --json
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "iceberg_evolve.cli",
            "diff",
            str(old),
            str(new),
            "--json",
        ],
        capture_output=True,
        text=True,
    )

    # 3. Assert it succeeded and printed exactly "[]"
    assert result.returncode == 0, f"CLI exited with {result.returncode}, stderr:\n{result.stderr}"
    out = result.stdout.strip()
    assert out == "[]", f"Expected empty list, got: {out!r}"


def test_cli_main_module_diff_human(tmp_path):
    # 1. Prepare schemas where new has one added column
    old = tmp_path / "old.json"
    new = tmp_path / "new.json"
    old.write_text(json.dumps({"fields": []}))
    new.write_text(json.dumps({
        "fields": [
            {"id": 1, "name": "col1", "type": "int", "required": True}
        ]
    }))

    # 2. Run without --json
    result = subprocess.run(
        [sys.executable, "-m", "iceberg_evolve.cli", "diff", str(old), str(new)],
        capture_output=True,
        text=True,
    )

    # 3. Expect at least one AddColumn line in the human-readable output
    assert result.returncode == 0
    assert "AddColumn" in result.stdout or "add" in result.stdout.lower()


from iceberg_evolve.cli import app
from iceberg_evolve.schema import Schema
from iceberg_evolve.serializer import IcebergSchemaJSONSerializer


def test_serialize_creates_file_and_outputs_message(monkeypatch, tmp_path):
    """
    Given valid catalog URL, table identifier, and output path,
    the serialize command should write the correct JSON schema to disk
    and output a confirmation message.
    """
    # Arrange: stub Schema.from_iceberg and serializer
    dummy_schema_obj = DummySchema(iceberg_schema="dummy_schema")
    dummy_dict = {"key": "value"}

    monkeypatch.setattr(Schema, "from_iceberg", lambda table_name, catalog, config: dummy_schema_obj)
    monkeypatch.setattr(IcebergSchemaJSONSerializer, "to_dict", lambda iceberg_schema: dummy_dict)

    output_file = tmp_path / "schema.json"

    # Act: invoke the CLI
    result = runner.invoke(
        app,
        [
            "serialize",
            "--catalog-url",
            "hive://localhost:9083",
            "--table-ident",
            "db.table",
            "--output-path",
            str(output_file),
        ],
    )

    # Assert: exit code, file content, and output message
    assert result.exit_code == 0
    assert output_file.exists(), f"Expected file at {output_file}"

    written = json.loads(output_file.read_text())
    assert written == dummy_dict, "JSON content does not match serializer output"

    assert "✅ Schema for 'db.table' written to" in result.stdout


def test_serialize_short_flags(monkeypatch, tmp_path):
    """
    Using the short flag options (-c, -t, -p),
    serialize should still write the JSON and confirm.
    """
    # Arrange
    dummy_schema_obj = DummySchema(iceberg_schema="x")
    dummy_dict = {"a": 1}
    monkeypatch.setattr(Schema, "from_iceberg", lambda table_name, catalog, config: dummy_schema_obj)
    monkeypatch.setattr(IcebergSchemaJSONSerializer, "to_dict", lambda iceberg_schema: dummy_dict)

    output_file = tmp_path / "out.json"

    # Act
    result = runner.invoke(
        app,
        [
            "serialize",
            "-c",
            "glue",
            "-t",
            "ns.tbl",
            "-p",
            str(output_file),
        ],
    )

    # Assert
    assert result.exit_code == 0
    assert output_file.exists()
    assert json.loads(output_file.read_text()) == dummy_dict
    assert "✅ Schema for 'ns.tbl' written to" in result.stdout


class DummyParam(SimpleNamespace):
    # mimic what typer.ClickParam will present
    pass

def test_parse_none_returns_none():
    result = _parse_json_config(ctx=None, param=DummyParam(name="config"), value=None)
    assert result is None

def test_parse_empty_string_returns_none():
    result = _parse_json_config(ctx=None, param=DummyParam(name="config"), value="")
    assert result is None

def test_parse_valid_json_string():
    raw = '{"foo": [1, 2, 3], "bar": {"nested": true}}'
    result = _parse_json_config(ctx=None, param=DummyParam(name="config"), value=raw)
    # JSON booleans come back as Python booleans
    assert result == {"foo": [1, 2, 3], "bar": {"nested": True}}

def test_parse_invalid_json_raises_badparameter():
    from typer import BadParameter
    bad = "{not: valid,,, json}"
    with pytest.raises(BadParameter) as excinfo:
        _parse_json_config(ctx=None, param=DummyParam(name="config"), value=bad)
    # ensure the exception mentions our flag name and JSON error
    msg = str(excinfo.value)
    assert "Invalid JSON for --config" in msg
    assert "Expecting property name enclosed in double quotes" in msg or "Expecting" in msg
