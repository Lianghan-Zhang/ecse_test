"""
Workload reader: scan and preprocess SQL files from workload directory.

Handles:
- Recursive directory traversal
- BOM removal
- Comment stripping (-- and /* */)
- Semicolon removal
- Multi-statement handling
"""

import re
from pathlib import Path
from dataclasses import dataclass, field

import sqlglot
from sqlglot import exp


@dataclass
class WorkloadQuery:
    """Represents a preprocessed SQL query from the workload."""
    source_sql_file: str  # Original file name (e.g., "q17.sql")
    raw_sql: str  # Original SQL content
    cleaned_sql: str  # Preprocessed SQL
    warnings: list[str] = field(default_factory=list)

    def is_valid(self) -> bool:
        """Return True if query has valid cleaned SQL."""
        return bool(self.cleaned_sql.strip())


def scan_workload_dir(workload_dir: Path, recursive: bool = True) -> list[Path]:
    """
    Scan workload directory for .sql files.

    Args:
        workload_dir: Path to directory containing SQL files
        recursive: If True, recursively scan subdirectories

    Returns:
        List of Path objects for each .sql file, sorted by name
    """
    if recursive:
        sql_files = sorted(workload_dir.rglob("*.sql"))
    else:
        sql_files = sorted(workload_dir.glob("*.sql"))
    return sql_files


def read_sql_file(sql_path: Path) -> str:
    """
    Read SQL content from a file, handling BOM.

    Args:
        sql_path: Path to .sql file

    Returns:
        SQL content as string with BOM removed
    """
    content = sql_path.read_text(encoding="utf-8-sig")  # utf-8-sig handles BOM
    return content


def strip_comments(sql: str) -> str:
    """
    Remove SQL comments from the query.

    Handles:
    - Single-line comments: -- ...
    - Block comments: /* ... */
    - Preserves strings (won't strip comments inside strings)

    Args:
        sql: SQL string

    Returns:
        SQL with comments removed
    """
    result = []
    i = 0
    n = len(sql)

    while i < n:
        # Check for string literals (single quote)
        if sql[i] == "'":
            j = i + 1
            while j < n:
                if sql[j] == "'" and j + 1 < n and sql[j + 1] == "'":
                    # Escaped quote
                    j += 2
                elif sql[j] == "'":
                    j += 1
                    break
                else:
                    j += 1
            result.append(sql[i:j])
            i = j
        # Check for double-quoted identifiers
        elif sql[i] == '"':
            j = i + 1
            while j < n:
                if sql[j] == '"' and j + 1 < n and sql[j + 1] == '"':
                    # Escaped quote
                    j += 2
                elif sql[j] == '"':
                    j += 1
                    break
                else:
                    j += 1
            result.append(sql[i:j])
            i = j
        # Check for backtick identifiers (Spark)
        elif sql[i] == '`':
            j = i + 1
            while j < n and sql[j] != '`':
                j += 1
            if j < n:
                j += 1  # Include closing backtick
            result.append(sql[i:j])
            i = j
        # Check for single-line comment
        elif sql[i:i+2] == '--':
            # Skip until end of line
            j = i + 2
            while j < n and sql[j] != '\n':
                j += 1
            # Keep the newline
            if j < n:
                result.append('\n')
                j += 1
            i = j
        # Check for block comment
        elif sql[i:i+2] == '/*':
            # Skip until */
            j = i + 2
            while j < n - 1 and sql[j:j+2] != '*/':
                j += 1
            if j < n - 1:
                j += 2  # Skip */
            else:
                j = n
            # Replace with space to preserve token separation
            result.append(' ')
            i = j
        else:
            result.append(sql[i])
            i += 1

    return ''.join(result)


def strip_trailing_semicolons(sql: str) -> str:
    """
    Remove trailing semicolons from SQL.

    Args:
        sql: SQL string

    Returns:
        SQL with trailing semicolons removed
    """
    return sql.rstrip().rstrip(';').rstrip()


def clean_sql(sql: str) -> str:
    """
    Apply all preprocessing steps to SQL.

    Args:
        sql: Raw SQL string

    Returns:
        Cleaned SQL string
    """
    # Strip comments
    sql = strip_comments(sql)
    # Strip trailing semicolons
    sql = strip_trailing_semicolons(sql)
    # Normalize whitespace (but preserve structure)
    sql = re.sub(r'\n\s*\n', '\n', sql)  # Remove empty lines
    sql = sql.strip()
    return sql


def is_select_or_with(stmt: exp.Expression) -> bool:
    """
    Check if a statement is a SELECT or WITH (CTE) query.

    Args:
        stmt: sqlglot expression

    Returns:
        True if statement is SELECT or WITH
    """
    return isinstance(stmt, (exp.Select, exp.Union, exp.Intersect, exp.Except))


def extract_first_query(
    sql: str,
    dialect: str = "spark",
) -> tuple[str | None, list[str]]:
    """
    Extract the first valid SELECT/WITH statement from SQL.

    Handles multi-statement SQL by taking the first parseable SELECT/WITH.

    Args:
        sql: Cleaned SQL string
        dialect: SQL dialect

    Returns:
        Tuple of (extracted SQL or None, list of warnings)
    """
    warnings: list[str] = []

    # Try parsing as multiple statements
    try:
        statements = sqlglot.parse(sql, dialect=dialect)
    except sqlglot.errors.ParseError as e:
        warnings.append(f"Parse error: {e}")
        return None, warnings

    if not statements:
        warnings.append("No statements found")
        return None, warnings

    # Filter for SELECT/WITH statements
    select_stmts = []
    other_stmts = []

    for i, stmt in enumerate(statements):
        if stmt is None:
            continue
        if is_select_or_with(stmt):
            select_stmts.append((i, stmt))
        else:
            other_stmts.append((i, stmt))

    if not select_stmts:
        warnings.append(f"No SELECT/WITH statements found, got: {[type(s).__name__ for _, s in other_stmts if s]}")
        return None, warnings

    # Take the first SELECT/WITH
    first_idx, first_stmt = select_stmts[0]

    # Warn about skipped statements
    if len(select_stmts) > 1:
        skipped = len(select_stmts) - 1
        warnings.append(f"Skipped {skipped} additional SELECT/WITH statement(s)")

    if other_stmts:
        other_types = [type(s).__name__ for _, s in other_stmts if s]
        warnings.append(f"Skipped non-SELECT statements: {other_types}")

    # Return the SQL representation
    return first_stmt.sql(dialect=dialect), warnings


def load_workload_query(
    sql_path: Path,
    dialect: str = "spark",
) -> WorkloadQuery:
    """
    Load and preprocess a single SQL file.

    Args:
        sql_path: Path to .sql file
        dialect: SQL dialect

    Returns:
        WorkloadQuery object
    """
    warnings: list[str] = []

    # Read raw SQL
    try:
        raw_sql = read_sql_file(sql_path)
    except Exception as e:
        return WorkloadQuery(
            source_sql_file=sql_path.name,
            raw_sql="",
            cleaned_sql="",
            warnings=[f"Failed to read file: {e}"],
        )

    # Clean SQL
    cleaned_sql = clean_sql(raw_sql)

    if not cleaned_sql:
        return WorkloadQuery(
            source_sql_file=sql_path.name,
            raw_sql=raw_sql,
            cleaned_sql="",
            warnings=["Empty SQL after cleaning"],
        )

    # Extract first SELECT/WITH statement
    extracted_sql, extract_warnings = extract_first_query(cleaned_sql, dialect)
    warnings.extend(extract_warnings)

    if extracted_sql is None:
        # Fall back to cleaned SQL if extraction failed
        extracted_sql = cleaned_sql

    return WorkloadQuery(
        source_sql_file=sql_path.name,
        raw_sql=raw_sql,
        cleaned_sql=extracted_sql,
        warnings=warnings,
    )


def load_workload(
    workload_dir: Path,
    dialect: str = "spark",
    recursive: bool = True,
) -> list[WorkloadQuery]:
    """
    Load all SQL files from workload directory.

    Args:
        workload_dir: Path to workload directory
        dialect: SQL dialect
        recursive: If True, recursively scan subdirectories

    Returns:
        List of WorkloadQuery objects
    """
    sql_files = scan_workload_dir(workload_dir, recursive=recursive)
    queries = []

    for sql_path in sql_files:
        query = load_workload_query(sql_path, dialect=dialect)
        queries.append(query)

    return queries
