import logging
from typing import Any

import pytest

from dsgrid.dataset.dataset_mapping_manager import DatasetMappingManager

from dsgrid.config.dimension_mapping_base import DimensionMappingType
from dsgrid.common import VALUE_COLUMN
from dsgrid.exceptions import DSGFileInputError, DSGInvalidDataset
from dsgrid.query.dataset_mapping_plan import DatasetMappingPlan
from dsgrid.ibis.functions import (
    aggregate_single_value,
    cache,
    get_runtime_session,
    is_dataframe_empty,
    unpersist,
)
from dsgrid.ibis.session import (
    DoubleType,
    IntegerType,
    LongType,
    ShortType,
    StructField,
    StructType,
    StringType,
    create_dataframe_from_dicts,
    use_duckdb,
)
from dsgrid.ibis.operations import filter_sql
from dsgrid.utils.dataset import (
    add_null_rows_from_load_data_lookup,
    apply_scaling_factor,
    convert_types_if_necessary,
    is_noop_mapping,
    merge_expected_associations_tables,
    remove_invalid_null_timestamps,
    repartition_if_needed_by_mapping,
    unpivot_dataframe,
)
from dsgrid.utils.scratch_dir_context import ScratchDirContext

from dsgrid.ibis.table_utils import count_rows
from tests._helpers import collect as _collect


@pytest.fixture(scope="module")
def tables():
    df = create_dataframe_from_dicts(
        [
            {
                "county": "Jefferson",
                "sector": "com",
                "com_elec": 2.1,
                "res_elec": None,
                "common_elec": 7.8,
            },
            {
                "county": "Boulder",
                "sector": "com",
                "com_elec": 3.5,
                "res_elec": None,
                "common_elec": 6.8,
            },
            {
                "county": "Denver",
                "sector": "res",
                "com_elec": None,
                "res_elec": 4.2,
                "common_elec": 5.8,
            },
            {
                "county": "Adams",
                "sector": "res",
                "com_elec": None,
                "res_elec": 1.3,
                "common_elec": 4.8,
            },
        ]
    )
    records = create_dataframe_from_dicts(
        [
            {"from_id": "res_elec", "to_id": "all_electricity", "from_fraction": 1.0},
            {"from_id": "com_elec", "to_id": "all_electricity", "from_fraction": 1.0},
            {
                "from_id": "common_elec",
                "to_id": "all_electricity",
                "from_fraction": 1.0,
            },
        ]
    )
    pivoted_columns = {"com_elec", "res_elec", "common_elec"}
    yield df, records, pivoted_columns


@pytest.fixture(scope="module")
def pivoted_dataframe_with_time():
    df = create_dataframe_from_dicts(
        [
            {
                "time_index": 0,
                "county": "Jefferson",
                "cooling": 2.1,
                "heating": 1.3,
            },
            {
                "time_index": 1,
                "county": "Jefferson",
                "cooling": 2.2,
                "heating": 1.4,
            },
            {
                "time_index": 3,
                "county": "Jefferson",
                "cooling": 2.3,
                "heating": 1.5,
            },
            {
                "time_index": 0,
                "county": "Boulder",
                "cooling": 1.1,
                "heating": None,
            },
            {
                "time_index": 1,
                "county": "Boulder",
                "cooling": 1.2,
                "heating": None,
            },
            {
                "time_index": 3,
                "county": "Boulder",
                "cooling": 1.3,
                "heating": None,
            },
        ]
    )
    df = cache(df)
    yield df, ["time_index"], ["cooling", "heating"]
    unpersist(df)


def test_is_noop_mapping_true():
    df = create_dataframe_from_dicts(
        [
            {
                "from_id": "elec_cooling",
                "to_id": "elec_cooling",
                "from_fraction": 1.0,
            },
            {
                "from_id": "elec_heating",
                "to_id": "elec_heating",
                "from_fraction": 1.0,
            },
        ]
    )
    assert is_noop_mapping(df)


def test_is_noop_mapping_false():
    for records in (
        [
            {
                "from_id": "elec_cooling",
                "to_id": "elec_cooling",
                "from_fraction": 1.0,
            },
            {
                "from_id": "electricity_heating",
                "to_id": "elec_heating",
                "from_fraction": 1.0,
            },
        ],
        [
            {
                "from_id": "elec_cooling",
                "to_id": "electricity_cooling",
                "from_fraction": 1.0,
            },
            {
                "from_id": "elec_heating",
                "to_id": "elec_heating",
                "from_fraction": 1.0,
            },
        ],
        [
            {
                "from_id": "elec_cooling",
                "to_id": "elec_cooling",
                "from_fraction": 2.0,
            },
            {
                "from_id": "elec_heating",
                "to_id": "elec_heating",
                "from_fraction": 1.0,
            },
        ],
        [
            {
                "from_id": "elec_cooling",
                "to_id": "elec_cooling",
                "from_fraction": 1.0,
            },
            {
                "from_id": "elec_heating",
                "to_id": "elec_heating",
                "from_fraction": 2.0,
            },
        ],
        [
            # NULLs are ignored
            {
                "from_id": "elec_cooling",
                "to_id": "elec_cooling",
                "from_fraction": 1.0,
            },
            {
                "from_id": "elec_cooling",
                "to_id": None,
                "from_fraction": 1.0,
            },
        ],
        [
            # NULLs are ignored
            {
                "from_id": "elec_cooling",
                "to_id": "elec_cooling",
                "from_fraction": 1.0,
            },
            {
                "from_id": None,
                "to_id": "elec_cooling",
                "from_fraction": 1.0,
            },
        ],
    ):
        df = create_dataframe_from_dicts(records)
        assert not is_noop_mapping(df)


def test_add_null_rows_from_load_data_lookup():
    spark = get_runtime_session()
    df = spark.createDataFrame(
        [
            ("2018-01-01 01:00:00", 2030, "Jefferson", 1.0),
            ("2018-01-01 02:00:00", 2030, "Jefferson", 2.0),
            ("2018-01-01 03:00:00", 2030, "Jefferson", 3.0),
        ],
        StructType(
            [
                StructField("timestamp", StringType(), True),
                StructField("model_year", IntegerType(), False),
                StructField("geography", StringType(), False),
                StructField("value", DoubleType(), True),
            ],
        ),
    )
    lookup = spark.createDataFrame(
        [
            (None, 2030, "Jefferson"),
            (None, 2030, "Boulder"),
        ],
        StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("model_year", IntegerType(), False),
                StructField("geography", StringType(), False),
            ],
        ),
    )
    result = add_null_rows_from_load_data_lookup(df, lookup)
    assert count_rows(result) == 4
    null_rows = _collect(filter_sql(result, "timestamp is NULL"))
    assert len(null_rows) == 1
    assert null_rows[0].geography == "Boulder"


def test_remove_invalid_null_timestamps():
    df = create_dataframe_from_dicts(
        [
            # No nulls
            {
                "timestamp": 1,
                "county": "Jefferson",
                "subsector": "warehouse",
                "value": 4,
            },
            {
                "timestamp": 2,
                "county": "Jefferson",
                "subsector": "warehouse",
                "value": 5,
            },
            # Nulls and valid values
            {
                "timestamp": None,
                "county": "Boulder",
                "subsector": "large_office",
                "value": 0,
            },
            {
                "timestamp": 1,
                "county": "Boulder",
                "subsector": "large_office",
                "value": 4,
            },
            {
                "timestamp": 2,
                "county": "Boulder",
                "subsector": "large_office",
                "value": 5,
            },
            # Only nulls
            {
                "timestamp": None,
                "county": "Adams",
                "subsector": "retail_stripmall",
                "value": 0,
            },
            {
                "timestamp": None,
                "county": "Denver",
                "subsector": "hospital",
                "value": 0,
            },
        ]
    )
    stacked = ["county", "subsector"]
    time_col = "timestamp"
    result = remove_invalid_null_timestamps(df, {time_col}, stacked)
    assert count_rows(result) == 6
    assert count_rows(filter_sql(result, "county == 'Boulder'")) == 2
    assert is_dataframe_empty(filter_sql(result, f"county == 'Boulder' and {time_col} is NULL"))


def test_apply_scaling_factor(tmp_path):
    df = create_dataframe_from_dicts(
        [
            {"value": 1, "bystander": 1, "scaling_factor": 5},
            {"value": 2, "bystander": 1, "scaling_factor": 6},
            {"value": 3, "bystander": 1, "scaling_factor": 0},
            {"value": 4, "bystander": 1, "scaling_factor": None},
        ],
    )
    dataset_id = "test_dataset"
    plan = DatasetMappingPlan(dataset_id=dataset_id)
    with DatasetMappingManager(dataset_id, plan, ScratchDirContext(tmp_path)) as mgr:
        df2 = apply_scaling_factor(df, "value", mgr)
    expected_sum = 1 * 5 + 2 * 6 + 0 + 4
    expected_sum_bystander = 1 + 1 + 1 + 1

    assert aggregate_single_value(df2, "sum", "value") == expected_sum
    assert aggregate_single_value(df2, "sum", "bystander") == expected_sum_bystander


@pytest.mark.skipif(use_duckdb(), reason="This feature is not used with DuckDB.")
def test_repartition_if_needed_by_mapping(tmp_path, caplog, tables):
    df = tables[0]
    context = ScratchDirContext(tmp_path)
    with caplog.at_level(logging.INFO):
        df, _ = repartition_if_needed_by_mapping(
            df,
            DimensionMappingType.ONE_TO_MANY_DISAGGREGATION,
            context,
        )
        assert "Completed repartition" in caplog.text


@pytest.mark.skipif(use_duckdb(), reason="This feature is not used with DuckDB.")
def test_repartition_if_needed_by_mapping_not_needed(tmp_path, caplog, tables):
    df = tables[0]
    context = ScratchDirContext(tmp_path)
    with caplog.at_level(logging.DEBUG):
        df, _ = repartition_if_needed_by_mapping(
            df,
            DimensionMappingType.ONE_TO_ONE,
            context,
        )
        assert "Repartition is not needed" in caplog.text
        assert "Completed repartition" not in caplog.text


def test_unpivot(pivoted_dataframe_with_time):
    df, time_columns, value_columns = pivoted_dataframe_with_time
    unpivoted = unpivot_dataframe(df, value_columns, "end_use", time_columns)
    expected_columns = [*time_columns, "county", "end_use", VALUE_COLUMN]
    assert list(unpivoted.columns) == expected_columns
    null_data = _collect(filter_sql(unpivoted, "county = 'Boulder' and end_use = 'heating'"))
    assert len(null_data) == 1
    assert null_data[0].time_index is None
    value = getattr(null_data[0], VALUE_COLUMN)
    assert value is None or value != value


@pytest.mark.parametrize("data_type", [IntegerType(), ShortType(), LongType()])
def test_convert_types_if_necessary(data_type):
    schema = StructType(
        [
            StructField("model_year", data_type, False),
            StructField("weather_year", data_type, False),
            StructField("bystander", IntegerType(), False),
        ]
    )
    df1 = get_runtime_session().createDataFrame([(2030, 2018, 2040)], schema)
    df2 = convert_types_if_necessary(df1)
    row = _first(df2)
    assert row.model_year == "2030"
    assert row.weather_year == "2018"
    assert row.bystander == 2040


# ---------------------------------------------------------------------------
# merge_expected_associations_tables tests
# ---------------------------------------------------------------------------


@pytest.fixture
def dim_records():
    """Complete dimension records for a small toy dataset."""
    return {
        "geography": ["A", "B", "C"],
        "sector": ["res", "com"],
        "subsector": ["sf", "mf", "office"],
    }


def _make_df(rows: list[dict[str, str]]):
    return create_dataframe_from_dicts(rows)


def _sorted_rows(df) -> list[Any]:
    """Collect an Ibis table into a sorted list of tuples for easy comparison."""
    cols = sorted(df.columns)
    return sorted(_collect(df.select(*cols).distinct()), key=lambda r: tuple(r))


def _first(df):
    rows = _collect(df.limit(1))
    return rows[0]


class TestMergeExpectedAssociationsTables:
    """Tests for merge_expected_associations_tables in dsgrid.utils.dataset."""

    def test_single_full_table(self, tmp_path, dim_records):
        """A single table covering all dimensions is returned as-is (minus dups)."""
        dfs = {
            "all": _make_df(
                [
                    {"geography": "A", "sector": "res", "subsector": "sf"},
                    {"geography": "B", "sector": "com", "subsector": "office"},
                    {"geography": "C", "sector": "res", "subsector": "mf"},
                    # Duplicate of first row — should be deduplicated.
                    {"geography": "A", "sector": "res", "subsector": "sf"},
                ]
            ),
        }
        with ScratchDirContext(tmp_path) as ctx:
            result = merge_expected_associations_tables(dfs, dim_records, ctx)
            rows = _sorted_rows(result)
            assert len(rows) == 3

    def test_identical_columns_union(self, tmp_path, dim_records):
        """Two tables with the same column set are unioned."""
        dfs = {
            "part1": _make_df(
                [
                    {"geography": "A", "sector": "res", "subsector": "sf"},
                    {"geography": "B", "sector": "com", "subsector": "office"},
                ]
            ),
            "part2": _make_df(
                [
                    {"geography": "C", "sector": "res", "subsector": "mf"},
                    # Overlap with part1 — should be deduplicated.
                    {"geography": "A", "sector": "res", "subsector": "sf"},
                ]
            ),
        }
        with ScratchDirContext(tmp_path) as ctx:
            result = merge_expected_associations_tables(dfs, dim_records, ctx)
            rows = _sorted_rows(result)
            assert len(rows) == 3

    def test_disjoint_columns_cross_join(self, tmp_path, dim_records):
        """Disjoint column sets are cross-joined, remaining dims filled in."""
        # Each table must include all records for its dimension columns.
        dfs = {
            "geo": _make_df([{"geography": "A"}, {"geography": "B"}, {"geography": "C"}]),
            "sector": _make_df([{"sector": "res"}, {"sector": "com"}]),
        }
        # subsector is uncovered -> cross-joined with all 3 records.
        with ScratchDirContext(tmp_path) as ctx:
            result = merge_expected_associations_tables(dfs, dim_records, ctx)
            rows = _sorted_rows(result)
            # 3 geo * 2 sector * 3 subsector = 18 (full cross-join)
            assert len(rows) == 18

    def test_overlapping_columns_inner_join(self, tmp_path, dim_records):
        """Overlapping-but-not-identical column sets are inner-joined on shared columns."""
        dfs = {
            "geo_sector": _make_df(
                [
                    {"geography": "A", "sector": "res"},
                    {"geography": "B", "sector": "com"},
                    {"geography": "C", "sector": "res"},
                ]
            ),
            "sector_sub": _make_df(
                [
                    {"sector": "res", "subsector": "sf"},
                    {"sector": "res", "subsector": "mf"},
                    {"sector": "com", "subsector": "office"},
                ]
            ),
        }
        with ScratchDirContext(tmp_path) as ctx:
            result = merge_expected_associations_tables(dfs, dim_records, ctx)
            rows = _sorted_rows(result)
            # geo A+C pair with res -> sf,mf (4); geo B pairs with com -> office (1)
            assert len(rows) == 5
            combos = {(r.geography, r.sector, r.subsector) for r in rows}
            assert ("A", "res", "mf") in combos
            assert ("B", "com", "office") in combos
            # geo B should NOT appear with res (only paired with com).
            assert ("B", "res", "sf") not in combos

    def test_partial_table_fills_remaining_dims(self, tmp_path, dim_records):
        """A single-column table cross-joins with full records of all other dims."""
        # Table must have all geography records.
        dfs = {
            "geo_only": _make_df([{"geography": "A"}, {"geography": "B"}, {"geography": "C"}]),
        }
        with ScratchDirContext(tmp_path) as ctx:
            result = merge_expected_associations_tables(dfs, dim_records, ctx)
            rows = _sorted_rows(result)
            # 3 geo * 2 sector * 3 subsector = 18
            assert len(rows) == 18

    def test_entry_check_fails_on_missing_record(self, tmp_path, dim_records):
        """A table missing a dimension record is caught at entry validation."""
        dfs = {
            "geo_sector": _make_df(
                [
                    {"geography": "A", "sector": "res"},
                    {"geography": "B", "sector": "com"},
                    # geography C is missing!
                ]
            ),
            "sector_sub": _make_df(
                [
                    {"sector": "res", "subsector": "sf"},
                    {"sector": "com", "subsector": "office"},
                ]
            ),
        }
        with ScratchDirContext(tmp_path) as ctx:
            with pytest.raises(DSGInvalidDataset, match="geography.*missing.*C"):
                merge_expected_associations_tables(dfs, dim_records, ctx)

    def test_entry_check_fails_on_missing_shared_value(self, tmp_path, dim_records):
        """A table missing a shared-column value is caught before the inner join."""
        dfs = {
            "geo_sector": _make_df(
                [
                    {"geography": "A", "sector": "res"},
                    {"geography": "B", "sector": "com"},
                    {"geography": "C", "sector": "res"},
                ]
            ),
            "sector_sub": _make_df(
                [
                    # Only "res" — "com" is missing from this table.
                    {"sector": "res", "subsector": "sf"},
                    {"sector": "res", "subsector": "mf"},
                ]
            ),
        }
        with ScratchDirContext(tmp_path) as ctx:
            with pytest.raises(DSGInvalidDataset, match="sector.*missing.*com"):
                merge_expected_associations_tables(dfs, dim_records, ctx)

    def test_entry_check_on_single_table(self, tmp_path, dim_records):
        """A single full-dim table missing a record is caught (first group)."""
        dfs = {
            "all": _make_df(
                [
                    {"geography": "A", "sector": "res", "subsector": "sf"},
                    {"geography": "B", "sector": "com", "subsector": "office"},
                    # geography C is missing
                ]
            ),
        }
        with ScratchDirContext(tmp_path) as ctx:
            with pytest.raises(DSGInvalidDataset, match="geography.*missing.*C"):
                merge_expected_associations_tables(dfs, dim_records, ctx)

    def test_union_partners_complement_each_other(self, tmp_path, dim_records):
        """Two tables with identical columns can individually be incomplete
        as long as their union covers all dimension records."""
        dfs = {
            "part1": _make_df(
                [
                    # Only geography A and sector res.
                    {"geography": "A", "sector": "res", "subsector": "sf"},
                ]
            ),
            "part2": _make_df(
                [
                    # Adds B, C, com, mf, office.
                    {"geography": "B", "sector": "com", "subsector": "mf"},
                    {"geography": "C", "sector": "res", "subsector": "office"},
                ]
            ),
        }
        with ScratchDirContext(tmp_path) as ctx:
            result = merge_expected_associations_tables(dfs, dim_records, ctx)
            rows = _sorted_rows(result)
            assert len(rows) == 3

    def test_three_groups_with_remaining_dims(self, tmp_path, dim_records):
        """Two overlapping groups + a remaining uncovered dimension."""
        dfs = {
            "geo_sector": _make_df(
                [
                    {"geography": "A", "sector": "res"},
                    {"geography": "B", "sector": "com"},
                    {"geography": "C", "sector": "res"},
                ]
            ),
            "sector_sub": _make_df(
                [
                    {"sector": "res", "subsector": "sf"},
                    {"sector": "res", "subsector": "mf"},
                    {"sector": "res", "subsector": "office"},
                    {"sector": "com", "subsector": "sf"},
                    {"sector": "com", "subsector": "mf"},
                    {"sector": "com", "subsector": "office"},
                ]
            ),
        }
        # Add a 4th dimension not covered by any table.
        dim_records_4d = {
            **dim_records,
            "model_year": ["2020", "2025"],
        }
        with ScratchDirContext(tmp_path) as ctx:
            result = merge_expected_associations_tables(dfs, dim_records_4d, ctx)
            rows = _sorted_rows(result)
            # Inner join: (A,res) + (C,res) get sf/mf/office (6); (B,com) gets sf/mf/office (3) = 9
            # * 2 model_years = 18
            assert len(rows) == 18

    def test_inner_join_drops_shared_column_value(self, tmp_path):
        """Inner join that drops a shared-column value is caught post-join.

        Both tables individually contain all records for every shared column
        (passing entry validation), but the inner join drops a value because
        the two tables have no matching rows for it.

        Table 1 (geo_sector):  sector=s3 pairs only with geography=C
        Table 2 (sector_sub):  sector=s3 pairs only with subsector=r

        Overlap is {sector}. Both tables have s1, s2, s3 for sector so entry
        validation passes. After inner join on {sector}, s3 survives. To
        actually lose a value we need a *multi-column* overlap where a value
        vanishes.

        Use a 2-column overlap {sector, subsector}:
        Table 1: sector=s1 only with subsector=p
        Table 2: sector=s1 only with subsector=q
        Inner join on {sector, subsector} finds no match for (s1, p) or
        (s1, q), so sector=s1 is dropped entirely.
        """
        dim_records = {
            "geography": ["A", "B"],
            "sector": ["s1", "s2"],
            "subsector": ["p", "q"],
        }

        dfs = {
            "geo_sector_sub": _make_df(
                [
                    # Has sector s1 only with subsector p, and s2 with both.
                    {"geography": "A", "sector": "s1", "subsector": "p"},
                    {"geography": "A", "sector": "s2", "subsector": "p"},
                    {"geography": "B", "sector": "s1", "subsector": "p"},
                    {"geography": "B", "sector": "s2", "subsector": "q"},
                ]
            ),
            "sector_sub": _make_df(
                [
                    # Has sector s1 only with subsector q (no match for s1+p).
                    # s2 appears with both p and q.
                    {"sector": "s1", "subsector": "q"},
                    {"sector": "s2", "subsector": "p"},
                    {"sector": "s2", "subsector": "q"},
                ]
            ),
        }
        with ScratchDirContext(tmp_path) as ctx:
            with pytest.raises(DSGInvalidDataset, match="Inner join.*dropped"):
                merge_expected_associations_tables(dfs, dim_records, ctx)

    def test_column_not_in_dim_records(self, tmp_path):
        """A table column not in all_dim_records raises DSGFileInputError."""
        dim_records = {
            "geography": ["A", "B"],
        }
        dfs = {
            "all": _make_df(
                [
                    {"geography": "A", "extra_col": "x"},
                    {"geography": "B", "extra_col": "y"},
                ]
            ),
        }
        with ScratchDirContext(tmp_path) as ctx:
            with pytest.raises(DSGFileInputError, match="Unexpected dimension type"):
                merge_expected_associations_tables(dfs, dim_records, ctx)
