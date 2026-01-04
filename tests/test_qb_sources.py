"""
Unit tests for qb_sources module.
"""

import pytest
from pathlib import Path

import sqlglot
from sqlglot import exp

from ecse_gen.qb_sources import (
    TableSource,
    QBSources,
    SourceExtractor,
    ColumnResolver,
    extract_sources_from_select,
    resolve_columns,
    get_cte_names_from_ast,
)
from ecse_gen.schema_meta import load_schema_meta


@pytest.fixture
def schema_meta():
    """Load test schema metadata."""
    schema_path = Path(__file__).parent.parent / "schema_meta.json"
    return load_schema_meta(schema_path)


class TestSourceExtraction:
    """Tests for source extraction."""

    def test_single_table(self):
        """Test extracting single table."""
        sql = "SELECT * FROM store_sales"
        ast = sqlglot.parse_one(sql, dialect="spark")

        sources = extract_sources_from_select(ast)

        assert len(sources.tables) == 1
        assert sources.tables[0].name == "store_sales"
        assert sources.tables[0].alias == "store_sales"
        assert sources.tables[0].kind == "base"

    def test_table_with_alias(self):
        """Test extracting table with alias."""
        sql = "SELECT * FROM store_sales ss"
        ast = sqlglot.parse_one(sql, dialect="spark")

        sources = extract_sources_from_select(ast)

        assert len(sources.tables) == 1
        assert sources.tables[0].name == "store_sales"
        assert sources.tables[0].alias == "ss"
        assert sources.tables[0].kind == "base"

    def test_multiple_tables_join(self):
        """Test extracting multiple tables from JOIN."""
        sql = """
        SELECT *
        FROM store_sales ss
        JOIN item i ON ss.ss_item_sk = i.i_item_sk
        LEFT JOIN date_dim dd ON ss.ss_sold_date_sk = dd.d_date_sk
        """
        ast = sqlglot.parse_one(sql, dialect="spark")

        sources = extract_sources_from_select(ast)

        assert len(sources.tables) == 3

        aliases = {t.alias for t in sources.tables}
        assert aliases == {"ss", "i", "dd"}

        names = {t.name for t in sources.tables}
        assert names == {"store_sales", "item", "date_dim"}

    def test_derived_subquery(self):
        """Test extracting derived subquery."""
        sql = "SELECT * FROM (SELECT a FROM t) AS sub"
        ast = sqlglot.parse_one(sql, dialect="spark")

        sources = extract_sources_from_select(ast)

        assert len(sources.tables) == 1
        assert sources.tables[0].kind == "derived"
        assert sources.tables[0].alias == "sub"
        assert sources.tables[0].name.startswith("__derived__")

    def test_derived_without_alias(self):
        """Test derived subquery without explicit alias."""
        sql = "SELECT * FROM (SELECT a FROM t)"
        ast = sqlglot.parse_one(sql, dialect="spark")

        sources = extract_sources_from_select(ast)

        assert len(sources.tables) == 1
        assert sources.tables[0].kind == "derived"
        # Should have synthetic alias
        assert sources.tables[0].alias.startswith("__derived__")

    def test_cte_reference(self):
        """Test detecting CTE reference."""
        sql = """
        WITH cte1 AS (SELECT 1)
        SELECT * FROM cte1
        """
        ast = sqlglot.parse_one(sql, dialect="spark")
        cte_names = get_cte_names_from_ast(ast)

        # Get the main SELECT (inside the WITH)
        main_select = ast.find(exp.Select)
        sources = extract_sources_from_select(main_select, cte_names=cte_names)

        assert len(sources.tables) == 1
        assert sources.tables[0].name == "cte1"
        assert sources.tables[0].kind == "cte_ref"

    def test_mixed_sources(self):
        """Test mixed base, CTE ref, and derived sources."""
        sql = """
        WITH my_cte AS (SELECT 1 AS x)
        SELECT *
        FROM store_sales ss
        JOIN my_cte ON ss.ss_item_sk = my_cte.x
        JOIN (SELECT * FROM item) AS derived_item ON ss.ss_item_sk = derived_item.i_item_sk
        """
        ast = sqlglot.parse_one(sql, dialect="spark")
        cte_names = get_cte_names_from_ast(ast)

        main_select = ast.find(exp.Select)
        sources = extract_sources_from_select(main_select, cte_names=cte_names)

        assert len(sources.tables) == 3

        kinds = {t.kind for t in sources.tables}
        assert kinds == {"base", "cte_ref", "derived"}

        # Check specific sources
        by_alias = {t.alias: t for t in sources.tables}
        assert by_alias["ss"].kind == "base"
        assert by_alias["my_cte"].kind == "cte_ref"
        assert by_alias["derived_item"].kind == "derived"


class TestQBSourcesMethods:
    """Tests for QBSources methods."""

    def test_get_source_by_alias(self):
        """Test getting source by alias."""
        sources = QBSources()
        source = TableSource(name="store_sales", alias="ss", kind="base")
        sources.add_source(source)

        assert sources.get_source_by_alias("ss") is source
        assert sources.get_source_by_alias("store_sales") is source
        assert sources.get_source_by_alias("unknown") is None

    def test_to_list(self):
        """Test converting to list of dicts."""
        sources = QBSources()
        sources.add_source(TableSource(name="t1", alias="a", kind="base"))
        sources.add_source(TableSource(name="t2", alias="b", kind="cte_ref"))

        result = sources.to_list()

        assert len(result) == 2
        assert result[0] == {"name": "t1", "alias": "a", "kind": "base"}
        assert result[1] == {"name": "t2", "alias": "b", "kind": "cte_ref"}


class TestColumnResolution:
    """Tests for column resolution."""

    def test_qualified_column_resolved(self):
        """Test resolving qualified column."""
        sql = "SELECT ss.ss_item_sk FROM store_sales ss"
        ast = sqlglot.parse_one(sql, dialect="spark")

        sources = extract_sources_from_select(ast)
        resolved, warnings = resolve_columns(ast, sources)

        assert len(resolved) == 1
        assert resolved[0].is_resolved
        assert resolved[0].table_alias == "ss"
        assert resolved[0].column_name == "ss_item_sk"

    def test_qualified_column_unknown_table(self):
        """Test qualified column with unknown table reference."""
        sql = "SELECT x.col FROM store_sales ss"
        ast = sqlglot.parse_one(sql, dialect="spark")

        sources = extract_sources_from_select(ast)
        resolved, warnings = resolve_columns(ast, sources)

        assert len(resolved) == 1
        assert not resolved[0].is_resolved
        assert "Unknown table reference" in resolved[0].warning

    def test_unqualified_column_with_schema(self, schema_meta):
        """Test resolving unqualified column with schema."""
        sql = "SELECT ss_item_sk FROM store_sales ss"
        ast = sqlglot.parse_one(sql, dialect="spark")

        sources = extract_sources_from_select(ast)
        resolved, warnings = resolve_columns(ast, sources, schema_meta)

        assert len(resolved) == 1
        assert resolved[0].is_resolved
        assert resolved[0].table_alias == "ss"
        assert resolved[0].column_name == "ss_item_sk"

    def test_unqualified_column_ambiguous(self, schema_meta):
        """Test ambiguous unqualified column."""
        # Both store_sales and item might have same column name in some schema
        # For this test, we use columns that only exist in one table
        sql = "SELECT ss_item_sk, i_item_sk FROM store_sales ss JOIN item i ON ss.ss_item_sk = i.i_item_sk"
        ast = sqlglot.parse_one(sql, dialect="spark")

        sources = extract_sources_from_select(ast)
        resolved, warnings = resolve_columns(ast, sources, schema_meta)

        # Both should resolve uniquely
        for col in resolved:
            assert col.is_resolved

    def test_unqualified_column_no_schema(self):
        """Test unqualified column without schema."""
        sql = "SELECT col FROM store_sales ss"
        ast = sqlglot.parse_one(sql, dialect="spark")

        sources = extract_sources_from_select(ast)
        resolved, warnings = resolve_columns(ast, sources, schema_meta=None)

        assert len(resolved) == 1
        assert not resolved[0].is_resolved
        assert "no schema" in resolved[0].warning.lower()

    def test_columns_not_in_subquery(self):
        """Test that columns in subqueries are not resolved at parent level."""
        sql = "SELECT a FROM t WHERE x IN (SELECT inner_col FROM inner_t)"
        ast = sqlglot.parse_one(sql, dialect="spark")

        sources = extract_sources_from_select(ast)
        resolved, warnings = resolve_columns(ast, sources)

        # Only 'a' and 'x' should be in scope, not 'inner_col'
        col_names = {r.column_name for r in resolved}
        assert "inner_col" not in col_names


class TestGetCteNames:
    """Tests for CTE name extraction."""

    def test_single_cte(self):
        """Test extracting single CTE name."""
        sql = "WITH cte1 AS (SELECT 1) SELECT * FROM cte1"
        ast = sqlglot.parse_one(sql, dialect="spark")

        cte_names = get_cte_names_from_ast(ast)

        assert cte_names == {"cte1"}

    def test_multiple_ctes(self):
        """Test extracting multiple CTE names."""
        sql = """
        WITH
            cte1 AS (SELECT 1),
            cte2 AS (SELECT 2)
        SELECT * FROM cte1, cte2
        """
        ast = sqlglot.parse_one(sql, dialect="spark")

        cte_names = get_cte_names_from_ast(ast)

        assert cte_names == {"cte1", "cte2"}

    def test_no_ctes(self):
        """Test query without CTEs."""
        sql = "SELECT * FROM t"
        ast = sqlglot.parse_one(sql, dialect="spark")

        cte_names = get_cte_names_from_ast(ast)

        assert cte_names == set()

    def test_cte_on_union(self):
        """Test CTE on UNION."""
        sql = """
        WITH cte AS (SELECT 1)
        SELECT * FROM cte
        UNION
        SELECT 2
        """
        ast = sqlglot.parse_one(sql, dialect="spark")

        cte_names = get_cte_names_from_ast(ast)

        assert cte_names == {"cte"}


class TestDerivedTableNaming:
    """Tests for derived table synthetic naming."""

    def test_stable_synthetic_names(self):
        """Test that synthetic names are stable across extractions."""
        sql = """
        SELECT *
        FROM (SELECT a FROM t1) AS d1
        JOIN (SELECT b FROM t2) AS d2 ON d1.a = d2.b
        """
        ast = sqlglot.parse_one(sql, dialect="spark")

        sources1 = extract_sources_from_select(ast)
        sources2 = extract_sources_from_select(ast)

        names1 = sorted([t.name for t in sources1.tables])
        names2 = sorted([t.name for t in sources2.tables])

        assert names1 == names2

    def test_derived_counter_increments(self):
        """Test that derived counter increments for multiple subqueries."""
        sql = """
        SELECT *
        FROM (SELECT a FROM t1) AS d1
        JOIN (SELECT b FROM t2) AS d2 ON d1.a = d2.b
        """
        ast = sqlglot.parse_one(sql, dialect="spark")

        sources = extract_sources_from_select(ast)
        derived = [t for t in sources.tables if t.kind == "derived"]

        assert len(derived) == 2
        names = {t.name for t in derived}
        assert "__derived__1" in names
        assert "__derived__2" in names


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
