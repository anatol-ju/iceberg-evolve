"""
Schema utilities for loading and normalizing JSON-style schemas.

PyIceberg documentation: https://py.iceberg.apache.org/configuration/

Examples:
    # Load from local JSON file
    schema = Schema.from_file("schemas/my_table.json")

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
import json
from schemaworks import JsonSchemaConverter

class Schema:
    def __init__(self, schema: dict):
        self.schema = schema
        self.normalize_fields()

    @property
    def fields(self):
        return self.schema.get("properties", {})

    def normalize_fields(self):
        """
        Normalize field names to lowercase and ensure they have 'name' and 'type'.
        """
        normalized = {}
        for name, spec in self.fields.items():
            if "type" not in spec:
                raise ValueError(f"Field '{name}' is missing a type")
            normalized[name.lower()] = spec
        self.schema["properties"] = dict(normalized.items())

    def __repr__(self):
        converter = JsonSchemaConverter(self.schema)
        dtypes = converter.to_dtypes(to_lower=True)
        return f"Schema({dtypes})"

    @classmethod
    def from_file(cls, path: str):
        """
        Load schema from a JSON file.

        Args:
            path (str): The path to the JSON file containing the schema.

        Returns:
            Schema: An instance of the Schema class with the loaded schema.

        Raises:
            FileNotFoundError: If the file does not exist.
            json.JSONDecodeError: If the file is not a valid JSON.
        """
        with open(path) as f:
            data = json.load(f)
        return cls(schema=data)

    @classmethod
    def from_iceberg(cls, table_name: str, catalog: str = "glue", config: dict = None):
        """
        Load schema from an Iceberg table in a catalog.

        Args:
            table_name (str): The name of the Iceberg table.
            catalog (str): The name of the Iceberg catalog. Defaults to "glue".
            config (dict): Optional catalog configuration overrides.

        Returns:
            Schema: An instance of the Schema class with the loaded schema.
        """
        from iceberg_evolve.catalog import load_table_schema
        schema = load_table_schema(table_name, catalog, config)
        return cls(schema=schema)

    @classmethod
    def from_s3(cls, bucket: str, key: str):
        """
        Load schema from an S3 bucket.

        Args:
            bucket (str): The name of the S3 bucket.
            key (str): The S3 key for the schema file.

        Returns:
            Schema: An instance of the Schema class with the loaded schema.

        Raises:
            boto3.exceptions.S3UploadFailedError: If the S3 object cannot be accessed.
            json.JSONDecodeError: If the S3 object is not a valid JSON.
        """
        import boto3
        s3 = boto3.resource("s3")
        obj = s3.Object(bucket, key)
        data = json.loads(obj.get()["Body"].read().decode("utf-8"))
        return cls(schema=data)
