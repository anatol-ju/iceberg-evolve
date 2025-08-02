import warnings
from dataclasses import dataclass, field

from pyiceberg.table.update.schema import UpdateSchema
from pyiceberg.types import IcebergType, ListType, StructType
from rich.console import Console
from rich.tree import Tree

from iceberg_evolve.exceptions import UnsupportedSchemaEvolutionWarning
from iceberg_evolve.utils import clean_type_str, is_narrower_than, type_to_tree


@dataclass
class BaseEvolutionOperation:
    """
    Base class for evolution operations.
    Defines the common interface for all operations.
    """
    name: str
    is_supported: bool = field(init=False)

    def __post_init__(self):
        self.is_supported = True

    def to_serializable_dict(self) -> dict:
        raise NotImplementedError

    def pretty(self, use_color: bool = False) -> Tree:
        raise NotImplementedError

    def apply(self, update: UpdateSchema) -> None:
        raise NotImplementedError

    def is_breaking(self) -> bool:
        return False

    def display(self, console: Console | None = None) -> None:
        from iceberg_evolve.renderer import EvolutionOperationsRenderer
        EvolutionOperationsRenderer([self], console).display()


@dataclass
class AddColumn(BaseEvolutionOperation):
    """
    Represents an operation to add a new column to the schema.

    Attributes:
        name (str): The name of the new column.
        is_supported (bool): Indicates if the operation is supported (by either this tool or PyIceberg).
        new_type (IcebergType): The type of the new column.
        doc (str | None): Optional documentation for the new column.
    """
    new_type: IcebergType
    doc: str | None = None

    def to_serializable_dict(self) -> dict:
        """
        Convert the operation to a serializable dictionary format.
        This is useful for serialization or logging.
        """
        return {
            "operation": "add_column",
            "name": self.name,
            "to": clean_type_str(self.new_type),
            **({"doc": self.doc} if self.doc else {}),
        }

    def pretty(self, use_color: bool = False) -> Tree:
        if use_color:
            label = "[green bold]ADD[/green bold]"
        else:
            label = "ADD"
        if not self.is_supported:
            label += " ⚠️"
        tree = Tree(label)
        tree.add(f"[green3]+ {self.name}[/green3]: {clean_type_str(self.new_type)}")
        return tree

    def apply(self, update: UpdateSchema) -> None:
        """
        Apply the add column operation to the schema update.

        Args:
            update (UpdateSchema): The schema update object to apply the operation to.
        """
        if not self.is_supported:
            warnings.warn(
                f"Skipping unsupported operation: {self.__class__.__name__} on '{self.name}' is not supported.",
                UnsupportedSchemaEvolutionWarning
            )
            return
        path = tuple(self.name.split(".")) if "." in self.name else self.name
        update.add_column(path, self.new_type, doc=self.doc)


@dataclass
class DropColumn(BaseEvolutionOperation):
    """
    Represents an operation to drop a column from the schema.

    Attributes:
        name (str): The name of the column to drop.
        is_supported (bool): Indicates if the operation is supported (by either this tool or PyIceberg).
    """
    def to_serializable_dict(self) -> dict:
        """
        Convert the operation to a serializable dictionary format.
        This is useful for serialization or logging.
        """
        return {
            "operation": "drop_column",
            "name": self.name
        }

    def pretty(self, use_color: bool = False) -> Tree:
        if use_color:
            label = "[red bold]DROP[/red bold]"
        else:
            label = "DROP"
        if not self.is_supported:
            label += " ⚠️"
        tree = Tree(label)
        tree.add(f"[red]- {self.name}[/red]")
        return tree

    def apply(self, update: UpdateSchema) -> None:
        """
        Apply the drop column operation to the schema update.

        Args:
            update (UpdateSchema): The schema update object to apply the operation to.
        """
        if not self.is_supported:
            warnings.warn(
                f"Skipping unsupported operation: {self.__class__.__name__} on '{self.name}' is not supported.",
                UnsupportedSchemaEvolutionWarning
            )
            return
        path = tuple(self.name.split(".")) if "." in self.name else self.name
        update.delete_column(path)

    def is_breaking(self) -> bool:
        """
        Check if dropping the column is a breaking change.
        This operation is always considered breaking because it removes data.

        Returns:
            bool: True if the operation is breaking, False otherwise.
        """
        return True


@dataclass
class UpdateColumn(BaseEvolutionOperation):
    """
    Represents an operation to update the type of an existing column.

    Attributes:
        name (str): The name of the column to update.
        is_supported (bool): Indicates if the operation is supported (by either this tool or PyIceberg).
        current_type (IcebergType): The current type of the column.
        new_type (IcebergType): The new type of the column.
        doc (str | None): Optional documentation for the column.
    """
    current_type: IcebergType
    new_type: IcebergType
    doc: str | None = None

    def __post_init__(self):
        self.is_supported = self.new_type.is_primitive

    def to_serializable_dict(self) -> dict:
        """
        Convert the operation to a serializable dictionary format.
        This is useful for serialization or logging.
        """
        return {
            "operation": "update_column_type",
            "name": self.name,
            "from": clean_type_str(self.current_type),
            "to": clean_type_str(self.new_type),
            **({"doc": self.doc} if self.doc else {}),
        }

    def pretty(self, use_color: bool = False) -> Tree:
        if use_color:
            label = "[yellow bold]UPDATE[/yellow bold]"
        else:
            label = "UPDATE"
        if not self.is_supported:
            label += " ⚠️"
        tree = Tree(label)
        node = tree.add(f"[yellow]~ {self.name}[/yellow]:")
        # from
        ct = self.current_type
        if isinstance(ct, StructType) or (isinstance(ct, ListType) and isinstance(ct.element_type, StructType)):
            node.add(type_to_tree("from", ct))
        else:
            node.add(f"from: {clean_type_str(ct)}")
        # to
        nt = self.new_type
        if isinstance(nt, StructType) or (isinstance(nt, ListType) and isinstance(nt.element_type, StructType)):
            node.add(type_to_tree("to", nt))
        else:
            node.add(f"to: {clean_type_str(nt)}")
        return tree

    def apply(self, update: UpdateSchema) -> None:
        """
        Apply the update column operation to the schema update.

        Args:
            update (UpdateSchema): The schema update object to apply the operation to.
        """
        path = tuple(self.name.split(".")) if "." in self.name else self.name
        if not self.is_supported:
            warnings.warn(
                (
                    f"Skipping unsupported operation: {self.__class__.__name__} on '{self.name}' is not supported.\n"
                    "Iceberg does not support changing nested types directly.\n"
                    "Suggested workaround: add a new column with the desired structure and migrate the data."
                ),
                UnsupportedSchemaEvolutionWarning
            )
            return
        update.update_column(path, field_type=self.new_type)

    def is_breaking(self) -> bool:
        """
        Check if updating the column type is a breaking change.
        This is the case if the new type is narrower than the current type.

        Returns:
            bool: True if the operation is breaking, False otherwise.
        """
        return is_narrower_than(self.new_type, self.current_type)


@dataclass
class RenameColumn(BaseEvolutionOperation):
    """
    Represents an operation to rename an existing column.

    Attributes:
        name (str): The current name of the column.
        is_supported (bool): Indicates if the operation is supported (by either this tool or PyIceberg).
        target (str): The new name for the column.
    """
    target: str

    def to_serializable_dict(self) -> dict:
        """
        Convert the operation to a serializable dictionary format.
        This is useful for serialization or logging.
        """
        return {
            "operation": "rename_column",
            "name": self.name,
            "to": self.target,
        }

    def pretty(self, use_color: bool = False) -> Tree:
        if use_color:
            label = "[cyan bold]RENAME[/cyan bold]"
        else:
            label = "RENAME"
        if not self.is_supported:
            label += " ⚠️"
        tree = Tree(label)
        subtree = tree.add(f"[cyan1]~ {self.name}[/cyan1]")
        subtree.add(f"to: {self.target}")
        return tree

    def apply(self, update: UpdateSchema) -> None:
        """
        Apply the rename column operation to the schema update.

        Args:
            update (UpdateSchema): The schema update object to apply the operation to.
        """
        if not self.is_supported:
            warnings.warn(
                f"Skipping unsupported operation: {self.__class__.__name__} on '{self.name}' is not supported.",
                UnsupportedSchemaEvolutionWarning
            )
            return
        path = tuple(self.name.split(".")) if "." in self.name else self.name
        update.rename_column(path, self.target)


@dataclass
class MoveColumn(BaseEvolutionOperation):
    """
    Represents an operation to move a column to a different position in the schema.

    Attributes:
        name (str): The name of the column to move.
        is_supported (bool): Indicates if the operation is supported (by either this tool or PyIceberg).
        target (str): The target column name to move before or after.
        position (str): The position to move the column to ("first", "before", or "after").
    """
    target: str
    position: str  # "first", "before", or "after"

    def to_serializable_dict(self) -> dict:
        """
        Convert the operation to a serializable dictionary format.
        This is useful for serialization or logging.
        """
        return {
            "operation": "move_column",
            "name": self.name,
            "position": self.position,
            "target": self.target,
        }

    def pretty(self, use_color: bool = False) -> Tree:
        if use_color:
            label = "[magenta bold]MOVE[/magenta bold]"
        else:
            label = "MOVE"
        if not self.is_supported:
            label += " ⚠️"
        tree = Tree(label)
        subtree = tree.add(f"[magenta3]~ {self.name}[/magenta3]")
        subtree.add(f"from: {self.position}")
        subtree.add(f"of: {self.target}")
        return tree

    def apply(self, update: UpdateSchema) -> None:
        """
        Apply the move column operation to the schema update.

        Args:
            update (UpdateSchema): The schema update object to apply the operation to.
        """
        if not self.is_supported:
            warnings.warn(
                f"Skipping unsupported operation: {self.__class__.__name__} on '{self.name}' is not supported.",
                UnsupportedSchemaEvolutionWarning
            )
            return
        path = tuple(self.name.split(".")) if "." in self.name else self.name
        if self.position == "first":
            update.move_first(path)
        elif self.position == "before":
            update.move_before(path, self.target)
        elif self.position == "after":
            update.move_after(path, self.target)


@dataclass
class UnionSchema(BaseEvolutionOperation):
    """
    Represents an operation to union a new schema with the current schema.

    Attributes:
        name (str): The name of the schema to union with.
        is_supported (bool): Indicates if the operation is supported (by either this tool or Py
        new_type (IcebergType): The new type to union with the current schema.
    """
    new_type: IcebergType

    def __post_init__(self):
        self.is_supported = False

    def to_serializable_dict(self) -> dict:
        """
        Convert the operation to a serializable dictionary format.
        This is useful for serialization or logging.
        """
        return {
            "operation": "union_schema",
            "with": clean_type_str(self.new_type),
        }

    def pretty(self, use_color: bool = False) -> Tree:
        if use_color:
            label = "[blue bold]UNION SCHEMA[/blue bold]"
        else:
            label = "UNION SCHEMA"
        if not self.is_supported:
            label += " ⚠️"
        tree = Tree(label)
        subtree = tree.add(f"[blue3]~ {self.name}[/blue3]:")
        subtree.add(f"with type: {clean_type_str(self.new_type)}")
        return tree

    def apply(self, update: UpdateSchema) -> None:
        """
        Apply the union schema operation to the schema update.
        This operation adds the new type to the current schema.

        Args:
            update (UpdateSchema): The schema update object to apply the operation to.
        """
        if not self.is_supported:
            warnings.warn(
                (
                    f"Skipping unsupported operation: {self.__class__.__name__} on '{self.name}' is not supported.\n"
                    "This feature is in development and not yet implemented."
                ),
                UnsupportedSchemaEvolutionWarning
            )
            return
        update.union_by_name(self.new_type)
