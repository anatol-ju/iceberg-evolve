class SchemaParseError(Exception):
    """Raised when an Iceberg schema file cannot be parsed correctly."""
    def __init__(self, message: str, path: str | None = None):
        super().__init__(message)
        self.path = path

class CatalogLoadError(Exception):
    """Raised when loading a schema from an Iceberg catalog fails."""
    def __init__(self, message: str, table: str, catalog: str):
        super().__init__(message)
        self.table = table
        self.catalog = catalog

class UnsupportedSchemaEvolutionWarning(UserWarning):
    """Raised when an unsupported schema evolution operation is attempted."""
    def __init__(self, message: str):
        super().__init__(message)
        self.message = message
