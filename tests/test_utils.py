"""Tests for shared utility consolidation."""

import pytest
from src.utils.language_constants import LanguageConstants
from src.utils.query_builders import BroadcastMonthQueryBuilder


class TestLanguageConstantsBuildCaseSql:
    """Tests for LanguageConstants.build_language_case_sql()."""

    def test_returns_valid_sql_case_statement(self):
        sql = LanguageConstants.build_language_case_sql()
        assert sql.strip().startswith("CASE")
        assert "END" in sql

    def test_uses_canonical_mappings(self):
        sql = LanguageConstants.build_language_case_sql()
        assert "'Chinese'" in sql
        assert "'Vietnamese'" in sql
        assert "'Filipino'" in sql
        assert "'Korean'" in sql
        assert "'Japanese'" in sql
        assert "'South Asian'" in sql
        assert "'Hmong'" in sql
        assert "'English'" in sql

    def test_p_maps_to_south_asian_not_portuguese(self):
        sql = LanguageConstants.build_language_case_sql()
        assert "'Portuguese'" not in sql

    def test_default_column_and_alias(self):
        sql = LanguageConstants.build_language_case_sql()
        assert "UPPER(TRIM(language_code))" in sql

    def test_custom_column(self):
        sql = LanguageConstants.build_language_case_sql(column="lang_code")
        assert "UPPER(TRIM(lang_code))" in sql
        assert "language_code" not in sql

    def test_custom_table_alias(self):
        sql = LanguageConstants.build_language_case_sql(
            table_alias="spots"
        )
        assert "UPPER(TRIM(spots.language_code))" in sql

    def test_no_table_alias(self):
        sql = LanguageConstants.build_language_case_sql(table_alias="")
        assert "UPPER(TRIM(language_code))" in sql
        assert ".language_code" not in sql

    def test_else_clause_present(self):
        sql = LanguageConstants.build_language_case_sql()
        assert "ELSE" in sql

    def test_all_language_group_codes_present(self):
        sql = LanguageConstants.build_language_case_sql()
        for code in LanguageConstants.LANGUAGE_GROUPS:
            assert f"'{code}'" in sql


class TestBroadcastMonthQueryBuilderYearFilter:
    """Tests for BroadcastMonthQueryBuilder.build_year_filter()."""

    def test_single_suffix(self):
        sql, params = BroadcastMonthQueryBuilder.build_year_filter(["24"])
        assert sql == "s.broadcast_month LIKE ?"
        assert params == ["%-24"]

    def test_multiple_suffixes(self):
        sql, params = BroadcastMonthQueryBuilder.build_year_filter(
            ["23", "24"]
        )
        assert "OR" in sql
        assert sql.startswith("(")
        assert sql.endswith(")")
        assert params == ["%-23", "%-24"]

    def test_custom_table_alias(self):
        sql, params = BroadcastMonthQueryBuilder.build_year_filter(
            ["24"], table_alias="spots"
        )
        assert "spots.broadcast_month" in sql

    def test_custom_month_column(self):
        sql, params = BroadcastMonthQueryBuilder.build_year_filter(
            ["24"], month_column="bcast_month"
        )
        assert "bcast_month" in sql

    def test_no_table_alias(self):
        sql, params = BroadcastMonthQueryBuilder.build_year_filter(
            ["24"], table_alias=""
        )
        assert sql == "broadcast_month LIKE ?"

    def test_three_suffixes(self):
        sql, params = BroadcastMonthQueryBuilder.build_year_filter(
            ["22", "23", "24"]
        )
        assert sql.count("LIKE ?") == 3
        assert len(params) == 3
