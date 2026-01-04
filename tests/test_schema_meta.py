"""
Unit tests for schema_meta module.
"""

import pytest
from pathlib import Path

from ecse_gen.schema_meta import load_schema_meta, SchemaMeta, ForeignKey


@pytest.fixture
def schema_path() -> Path:
    """Return path to test schema_meta.json."""
    return Path(__file__).parent.parent / "schema_meta.json"


@pytest.fixture
def schema(schema_path: Path) -> SchemaMeta:
    """Load and return SchemaMeta."""
    return load_schema_meta(schema_path)


class TestSchemaMetaLoading:
    """Tests for schema loading."""

    def test_load_tables(self, schema: SchemaMeta):
        """Test that tables are loaded correctly."""
        assert "store_sales" in schema.tables
        assert "item" in schema.tables
        assert "date_dim" in schema.tables
        assert "customer" in schema.tables

    def test_load_columns(self, schema: SchemaMeta):
        """Test that columns are loaded correctly."""
        ss = schema.tables["store_sales"]
        assert "ss_sold_date_sk" in ss.columns
        assert "ss_item_sk" in ss.columns
        assert "ss_customer_sk" in ss.columns

    def test_load_pk(self, schema: SchemaMeta):
        """Test that primary keys are loaded correctly."""
        assert schema.get_pk("store_sales") == ("ss_item_sk", "ss_ticket_number")
        assert schema.get_pk("item") == ("i_item_sk",)
        assert schema.get_pk("date_dim") == ("d_date_sk",)

    def test_load_role(self, schema: SchemaMeta):
        """Test that roles are loaded correctly."""
        assert schema.get_role("store_sales") == "fact"
        assert schema.get_role("item") == "dimension"
        assert schema.get_role("date_dim") == "dimension"

    def test_load_fks(self, schema: SchemaMeta):
        """Test that foreign keys are loaded correctly."""
        assert len(schema.foreign_keys) > 0
        # Check enforced FK
        fk = schema.get_fk("store_sales", "ss_item_sk", "item", "i_item_sk")
        assert fk is not None
        assert fk.enforced is True


class TestNotNullIndex:
    """Tests for NOT NULL index."""

    def test_pk_columns_not_null(self, schema: SchemaMeta):
        """Test that PK columns are NOT NULL."""
        assert schema.is_not_null("store_sales", "ss_item_sk")
        assert schema.is_not_null("store_sales", "ss_ticket_number")
        assert schema.is_not_null("item", "i_item_sk")
        assert schema.is_not_null("date_dim", "d_date_sk")

    def test_nullable_columns(self, schema: SchemaMeta):
        """Test that nullable columns are correctly identified."""
        assert not schema.is_not_null("store_sales", "ss_sold_date_sk")
        assert not schema.is_not_null("store_sales", "ss_customer_sk")
        assert not schema.is_not_null("item", "i_product_name")

    def test_unknown_table(self, schema: SchemaMeta):
        """Test behavior for unknown table."""
        assert not schema.is_not_null("unknown_table", "some_col")


class TestFKPairLookup:
    """Tests for FK pair lookup."""

    def test_enforced_fk(self, schema: SchemaMeta):
        """Test lookup of enforced FK."""
        assert schema.find_fk_pair("store_sales", "ss_item_sk", "item", "i_item_sk")
        assert schema.find_fk_pair("store_sales", "ss_sold_date_sk", "date_dim", "d_date_sk")
        assert schema.find_fk_pair("store_sales", "ss_customer_sk", "customer", "c_customer_sk")

    def test_recommended_fk(self, schema: SchemaMeta):
        """Test lookup of recommended (non-enforced) FK."""
        # These are recommended FKs with enforced=false
        assert schema.find_fk_pair("customer", "c_current_cdemo_sk", "customer_demographics", "cd_demo_sk")
        assert schema.find_fk_pair("customer", "c_current_hdemo_sk", "household_demographics", "hd_demo_sk")
        assert schema.find_fk_pair("customer", "c_current_addr_sk", "customer_address", "ca_address_sk")

    def test_recommended_fk_properties(self, schema: SchemaMeta):
        """Test that recommended FKs have correct properties."""
        fk = schema.get_fk("customer", "c_current_cdemo_sk", "customer_demographics", "cd_demo_sk")
        assert fk is not None
        assert fk.enforced is False
        assert fk.recommended is True

    def test_nonexistent_fk(self, schema: SchemaMeta):
        """Test lookup of non-existent FK."""
        assert not schema.find_fk_pair("store_sales", "ss_quantity", "item", "i_item_sk")
        assert not schema.find_fk_pair("unknown", "col", "table", "col")


class TestFKCompositeLookup:
    """Tests for composite FK lookup."""

    def test_simple_fk_as_composite(self, schema: SchemaMeta):
        """Test that simple FKs can be looked up as composite."""
        assert schema.find_fk_composite(
            "store_sales",
            ("ss_item_sk",),
            "item",
            ("i_item_sk",),
        )

    def test_simple_fk_with_list(self, schema: SchemaMeta):
        """Test that list args work for composite lookup."""
        assert schema.find_fk_composite(
            "store_sales",
            ["ss_item_sk"],
            "item",
            ["i_item_sk"],
        )

    def test_nonexistent_composite(self, schema: SchemaMeta):
        """Test non-existent composite FK."""
        assert not schema.find_fk_composite(
            "store_sales",
            ("ss_item_sk", "ss_customer_sk"),
            "item",
            ("i_item_sk", "i_brand"),
        )


class TestFKFromTable:
    """Tests for getting all FKs from a table."""

    def test_store_sales_fks(self, schema: SchemaMeta):
        """Test getting all FKs from store_sales."""
        fks = schema.get_fks_from_table("store_sales")
        assert len(fks) >= 8  # store_sales has many FKs

        # Check specific FKs
        to_tables = {fk.to_table for fk in fks}
        assert "item" in to_tables
        assert "date_dim" in to_tables
        assert "customer" in to_tables
        assert "store" in to_tables

    def test_customer_fks(self, schema: SchemaMeta):
        """Test getting all FKs from customer (includes recommended)."""
        fks = schema.get_fks_from_table("customer")
        assert len(fks) >= 5  # customer has several recommended FKs

        # Check that recommended FKs are included
        recommended_count = sum(1 for fk in fks if fk.recommended)
        assert recommended_count >= 1

    def test_no_fks_from_dimension(self, schema: SchemaMeta):
        """Test that dimension tables may have no outgoing FKs."""
        fks = schema.get_fks_from_table("date_dim")
        # date_dim has no FKs in our schema
        assert len(fks) == 0


class TestHasTableColumn:
    """Tests for table/column existence checks."""

    def test_has_table(self, schema: SchemaMeta):
        """Test has_table method."""
        assert schema.has_table("store_sales")
        assert schema.has_table("item")
        assert not schema.has_table("nonexistent_table")

    def test_has_column(self, schema: SchemaMeta):
        """Test has_column method."""
        assert schema.has_column("store_sales", "ss_item_sk")
        assert schema.has_column("item", "i_product_name")
        assert not schema.has_column("store_sales", "nonexistent_col")
        assert not schema.has_column("nonexistent_table", "col")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
