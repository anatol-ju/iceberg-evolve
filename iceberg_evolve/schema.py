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

    def __repr__(self):
        return f"IcebergSchema({self.iceberg_schema})"

    @classmethod
    def from_file(cls, path: str) -> "Schema":
        """
        Load schema from a JSON file.
        """
        if not path.lower().endswith(".json"):
            raise ValueError("Only JSON schema files are supported.")
        with open(path) as f:
            data = json.load(f)
        iceberg_schema = IcebergSchemaSerializer.from_dict(data)
        return cls(iceberg_schema)

    @classmethod
    def from_iceberg(cls, table_name: str, catalog: str = "glue", config: dict = None) -> "Schema":
        """
        Load schema from an Iceberg table in a catalog.
        """
        from iceberg_evolve.catalog import load_table_schema
        data = load_table_schema(table_name, catalog, config)
        iceberg_schema = IcebergSchemaSerializer.from_dict(data)
        return cls(iceberg_schema)

    @classmethod
    def from_s3(cls, bucket: str, key: str) -> "Schema":
        """
        Load schema from an S3 bucket.
        """
        if not key.lower().endswith(".json"):
            raise ValueError("Only JSON schema files are supported for schema loading from S3.")
        import boto3
        s3 = boto3.resource("s3")
        obj = s3.Object(bucket, key)
        data = json.loads(obj.get()["Body"].read().decode("utf-8"))
        iceberg_schema = IcebergSchemaSerializer.from_dict(data)
        return cls(iceberg_schema)
