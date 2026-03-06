"""Tests for shared utility consolidation."""

import pytest
from src.utils.language_constants import LanguageConstants
from src.utils.query_builders import (
    BroadcastMonthQueryBuilder,
    CustomerNormalizationQueryBuilder,
    RevenueQueryBuilder,
)


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


class TestRevenueQueryBuilder:
    """Tests for RevenueQueryBuilder imported from query_builders."""

    def test_build_broadcast_month_case_january(self):
        sql = RevenueQueryBuilder.build_broadcast_month_case()
        assert "WHEN s.broadcast_month LIKE 'Jan-%' THEN '01'" in sql

    def test_build_quarter_case_q1(self):
        sql = RevenueQueryBuilder.build_quarter_case()
        assert "'Q1'" in sql
        assert "Jan" in sql

    def test_build_year_case_2024(self):
        sql = RevenueQueryBuilder.build_year_case()
        assert "WHEN broadcast_month LIKE '%-24' THEN 2024" in sql

    def test_build_base_filters_excludes_trade(self):
        sql = RevenueQueryBuilder.build_base_filters()
        assert "Trade" in sql
        assert "revenue_type" in sql

    def test_build_ae_normalization(self):
        sql = RevenueQueryBuilder.build_ae_normalization()
        assert "UPPER(TRIM" in sql
        assert "UNKNOWN" in sql


class TestRevenueQueryBuilderNewMethods:
    """Tests for new month/quarter number helpers."""

    def test_build_month_number_case_january_returns_1(self):
        sql = RevenueQueryBuilder.build_month_number_case()
        assert "WHEN 'Jan' THEN 1" in sql

    def test_build_month_number_case_december_returns_12(self):
        sql = RevenueQueryBuilder.build_month_number_case()
        assert "WHEN 'Dec' THEN 12" in sql

    def test_build_month_number_case_custom_expr(self):
        sql = RevenueQueryBuilder.build_month_number_case("broadcast_month")
        assert "SUBSTR(broadcast_month, 1, 3)" in sql

    def test_build_quarter_number_case_q1_returns_1(self):
        sql = RevenueQueryBuilder.build_quarter_number_case()
        assert "THEN 1" in sql
        assert "'Jan'" in sql
        assert "'Feb'" in sql
        assert "'Mar'" in sql

    def test_build_quarter_number_case_q4_returns_4(self):
        sql = RevenueQueryBuilder.build_quarter_number_case()
        assert "ELSE 4" in sql
