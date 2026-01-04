"""
Schema metadata loader: load and parse schema_meta.json.

Provides indexes for efficient FK/PK/nullable lookups.
"""

import json
from pathlib import Path
from dataclasses import dataclass, field


@dataclass
class ColumnMeta:
    """Metadata for a single column."""
    name: str
    nullable: bool = True


@dataclass
class ForeignKey:
    """Represents a foreign key relationship (supports composite keys)."""
    from_table: str
    from_columns: tuple[str, ...]  # Tuple for hashability
    to_table: str
    to_columns: tuple[str, ...]
    enforced: bool = True
    recommended: bool = False

    def is_simple(self) -> bool:
        """Return True if this is a single-column FK."""
        return len(self.from_columns) == 1

    @property
    def from_column(self) -> str:
        """Return single from_column (for simple FKs)."""
        if not self.is_simple():
            raise ValueError("Cannot get single from_column for composite FK")
        return self.from_columns[0]

    @property
    def to_column(self) -> str:
        """Return single to_column (for simple FKs)."""
        if not self.is_simple():
            raise ValueError("Cannot get single to_column for composite FK")
        return self.to_columns[0]


@dataclass
class TableMeta:
    """Metadata for a single table."""
    name: str
    columns: dict[str, ColumnMeta]
    primary_key: tuple[str, ...] | None = None
    role: str | None = None  # "fact" or "dimension"


@dataclass
class SchemaMeta:
    """
    Complete schema metadata with indexes for efficient lookups.

    Indexes:
    - not_null_set[table] -> set of NOT NULL column names
    - pk_cols[table] -> tuple of PK column names
    - fk_by_pair[(child_table, child_col, parent_table, parent_col)] -> list[ForeignKey]
    - fk_by_childcols[(child_table, tuple(child_cols), parent_table)] -> ForeignKey
    - col_to_tables[column_name] -> set of table names that have this column
    """
    tables: dict[str, TableMeta]
    foreign_keys: list[ForeignKey]

    # Indexes (built after loading)
    not_null_set: dict[str, set[str]] = field(default_factory=dict)
    pk_cols: dict[str, tuple[str, ...]] = field(default_factory=dict)
    fk_by_pair: dict[tuple[str, str, str, str], list[ForeignKey]] = field(default_factory=dict)
    fk_by_childcols: dict[tuple[str, tuple[str, ...], str], ForeignKey] = field(default_factory=dict)
    # Additional index: all FKs from a child table
    fk_by_child_table: dict[str, list[ForeignKey]] = field(default_factory=dict)
    # Column to tables index: column_name -> set of table names
    col_to_tables: dict[str, set[str]] = field(default_factory=dict)

    def build_indexes(self) -> None:
        """Build all indexes from the loaded data."""
        # Build not_null_set, pk_cols, and col_to_tables
        for table_name, table_meta in self.tables.items():
            # NOT NULL columns
            not_null_cols = {
                col_name
                for col_name, col_meta in table_meta.columns.items()
                if not col_meta.nullable
            }
            self.not_null_set[table_name] = not_null_cols

            # PK columns
            if table_meta.primary_key:
                self.pk_cols[table_name] = table_meta.primary_key

            # Column to tables index
            for col_name in table_meta.columns:
                col_lower = col_name.lower()
                if col_lower not in self.col_to_tables:
                    self.col_to_tables[col_lower] = set()
                self.col_to_tables[col_lower].add(table_name)

        # Build FK indexes
        for fk in self.foreign_keys:
            # fk_by_childcols index
            key = (fk.from_table, fk.from_columns, fk.to_table)
            self.fk_by_childcols[key] = fk

            # fk_by_pair index (for simple FKs, also index by single column pair)
            if fk.is_simple():
                pair_key = (fk.from_table, fk.from_columns[0], fk.to_table, fk.to_columns[0])
                if pair_key not in self.fk_by_pair:
                    self.fk_by_pair[pair_key] = []
                self.fk_by_pair[pair_key].append(fk)

            # fk_by_child_table index
            if fk.from_table not in self.fk_by_child_table:
                self.fk_by_child_table[fk.from_table] = []
            self.fk_by_child_table[fk.from_table].append(fk)

    def is_not_null(self, table: str, col: str) -> bool:
        """
        Check if a column is NOT NULL.

        Args:
            table: Table name
            col: Column name

        Returns:
            True if column is NOT NULL, False otherwise
        """
        if table not in self.not_null_set:
            return False
        return col in self.not_null_set[table]

    def find_fk_pair(
        self,
        child_table: str,
        child_col: str,
        parent_table: str,
        parent_col: str,
    ) -> bool:
        """
        Check if a FK exists for the given column pair.

        Matches both enforced and recommended (non-enforced) FKs.

        Args:
            child_table: Child (referencing) table
            child_col: Child column
            parent_table: Parent (referenced) table
            parent_col: Parent column

        Returns:
            True if FK exists
        """
        pair_key = (child_table, child_col, parent_table, parent_col)
        return pair_key in self.fk_by_pair

    def find_fk_composite(
        self,
        child_table: str,
        child_cols: tuple[str, ...] | list[str],
        parent_table: str,
        parent_cols: tuple[str, ...] | list[str],
    ) -> bool:
        """
        Check if a composite FK exists.

        Matches both enforced and recommended (non-enforced) FKs.

        Args:
            child_table: Child (referencing) table
            child_cols: Child columns (in order)
            parent_table: Parent (referenced) table
            parent_cols: Parent columns (in order)

        Returns:
            True if FK exists
        """
        # Normalize to tuples
        child_cols_tuple = tuple(child_cols) if isinstance(child_cols, list) else child_cols
        parent_cols_tuple = tuple(parent_cols) if isinstance(parent_cols, list) else parent_cols

        key = (child_table, child_cols_tuple, parent_table)
        if key not in self.fk_by_childcols:
            return False

        fk = self.fk_by_childcols[key]
        return fk.to_columns == parent_cols_tuple

    def get_fk(
        self,
        child_table: str,
        child_col: str,
        parent_table: str,
        parent_col: str,
    ) -> ForeignKey | None:
        """
        Get FK object for the given column pair.

        Args:
            child_table: Child (referencing) table
            child_col: Child column
            parent_table: Parent (referenced) table
            parent_col: Parent column

        Returns:
            ForeignKey object if found, None otherwise
        """
        pair_key = (child_table, child_col, parent_table, parent_col)
        fks = self.fk_by_pair.get(pair_key)
        if fks:
            return fks[0]
        return None

    def get_pk(self, table: str) -> tuple[str, ...] | None:
        """
        Get primary key columns for a table.

        Args:
            table: Table name

        Returns:
            Tuple of PK column names, or None if no PK
        """
        return self.pk_cols.get(table)

    def get_role(self, table: str) -> str | None:
        """
        Get the role (fact/dimension) for a table.

        Args:
            table: Table name

        Returns:
            Role string or None
        """
        if table in self.tables:
            return self.tables[table].role
        return None

    def get_fks_from_table(self, child_table: str) -> list[ForeignKey]:
        """
        Get all FKs originating from a table.

        Args:
            child_table: Child table name

        Returns:
            List of ForeignKey objects
        """
        return self.fk_by_child_table.get(child_table, [])

    def has_table(self, table: str) -> bool:
        """Check if a table exists in the schema."""
        return table in self.tables

    def has_column(self, table: str, col: str) -> bool:
        """Check if a column exists in a table."""
        if table not in self.tables:
            return False
        return col in self.tables[table].columns

    def resolve_column(
        self,
        col_name: str,
        candidate_tables: set[str] | None = None,
    ) -> str | None:
        """
        Resolve an unqualified column name to its table.

        Args:
            col_name: Column name (case-insensitive)
            candidate_tables: If provided, only search within these tables.
                              If None, search all tables.

        Returns:
            Table name if column is unique within candidates, None otherwise.
            Returns None if:
            - Column not found in any candidate table
            - Column exists in multiple candidate tables (ambiguous)
        """
        col_lower = col_name.lower()

        # Get all tables that have this column
        tables_with_col = self.col_to_tables.get(col_lower, set())

        if not tables_with_col:
            return None

        # Filter by candidate tables if provided
        if candidate_tables is not None:
            tables_with_col = tables_with_col & candidate_tables

        if len(tables_with_col) == 1:
            return next(iter(tables_with_col))

        # Ambiguous or not found
        return None

    def resolve_column_with_info(
        self,
        col_name: str,
        candidate_tables: set[str] | None = None,
    ) -> tuple[str | None, str]:
        """
        Resolve column with detailed result info.

        Returns:
            Tuple of (table_name, status) where status is:
            - "unique": column found in exactly one table
            - "ambiguous": column found in multiple tables
            - "not_found": column not found in any candidate table
        """
        col_lower = col_name.lower()
        tables_with_col = self.col_to_tables.get(col_lower, set())

        if not tables_with_col:
            return None, "not_found"

        if candidate_tables is not None:
            tables_with_col = tables_with_col & candidate_tables

        if len(tables_with_col) == 0:
            return None, "not_found"
        elif len(tables_with_col) == 1:
            return next(iter(tables_with_col)), "unique"
        else:
            return None, "ambiguous"


def load_schema_meta(schema_path: Path) -> SchemaMeta:
    """
    Load schema metadata from JSON file.

    Args:
        schema_path: Path to schema_meta.json

    Returns:
        SchemaMeta object with indexes built
    """
    content = json.loads(schema_path.read_text(encoding="utf-8"))

    tables: dict[str, TableMeta] = {}
    foreign_keys: list[ForeignKey] = []

    # Parse tables
    for table_name, table_data in content.get("tables", {}).items():
        columns: dict[str, ColumnMeta] = {}

        # Handle columns - can be dict (new format) or list (old format)
        cols_data = table_data.get("columns", {})
        if isinstance(cols_data, dict):
            for col_name, col_info in cols_data.items():
                nullable = col_info.get("nullable", True) if isinstance(col_info, dict) else True
                columns[col_name] = ColumnMeta(name=col_name, nullable=nullable)
        elif isinstance(cols_data, list):
            # Old format: list of column names (assume nullable)
            for col_name in cols_data:
                columns[col_name] = ColumnMeta(name=col_name, nullable=True)

        # Parse primary key
        pk_data = table_data.get("primary_key")
        pk = tuple(pk_data) if pk_data else None

        # Parse role
        role = table_data.get("role")

        tables[table_name] = TableMeta(
            name=table_name,
            columns=columns,
            primary_key=pk,
            role=role,
        )

    # Parse foreign keys
    for fk_data in content.get("foreign_keys", []):
        # Support both old format (from_column/to_column) and new format (from_columns/to_columns)
        from_cols = fk_data.get("from_columns") or [fk_data.get("from_column")]
        to_cols = fk_data.get("to_columns") or [fk_data.get("to_column")]

        fk = ForeignKey(
            from_table=fk_data["from_table"],
            from_columns=tuple(from_cols),
            to_table=fk_data["to_table"],
            to_columns=tuple(to_cols),
            enforced=fk_data.get("enforced", True),
            recommended=fk_data.get("recommended", False),
        )
        foreign_keys.append(fk)

    schema = SchemaMeta(tables=tables, foreign_keys=foreign_keys)
    schema.build_indexes()

    return schema
