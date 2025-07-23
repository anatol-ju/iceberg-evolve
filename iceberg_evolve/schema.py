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
            "region": "us-west-2"
        }
    )
"""
from iceberg_evolve.utils import IcebergSchemaSerializer
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.catalog import load_catalog
import json

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
        Load schema from a JSON file.
        """
        if not path.lower().endswith(".json"):
            raise ValueError("Currently, only JSON files are supported for schema loading.")
        with open(path) as f:
            data = json.load(f)
        iceberg_schema = IcebergSchemaSerializer.from_dict(data)
        return cls(iceberg_schema)

    @classmethod
    def from_iceberg(cls, table_name: str, catalog: str = "glue", config: dict = None) -> "Schema":
        """
        Load schema from an Iceberg table in a catalog using PyIceberg.
        """
        catalog_kwargs = config or {}
        catalog_client = load_catalog(catalog, **catalog_kwargs)
        table = catalog_client.load_table(table_name)
        # PyIceberg Table.schema returns a pyiceberg.schema.Schema
        iceberg_schema = table.schema
        return cls(iceberg_schema)

    @classmethod
    def from_s3(cls, bucket: str, key: str) -> "Schema":
        """
        Load schema from an S3 bucket.
        """
        if not key.lower().endswith(".json"):
            raise ValueError("Currently, only JSON files are supported for schema loading from S3.")
        import boto3
        s3 = boto3.resource("s3")
        obj = s3.Object(bucket, key)
        data = json.loads(obj.get()["Body"].read().decode("utf-8"))
        iceberg_schema = IcebergSchemaSerializer.from_dict(data)
        return cls(iceberg_schema)
