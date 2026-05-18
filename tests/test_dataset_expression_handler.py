import pytest

from dsgrid.dataset.dataset_expression_handler import (
    DatasetExpressionHandler,
    evaluate_expression,
    join_multiple_columns,
)
from dsgrid.exceptions import DSGInvalidOperation
from dsgrid.ibis.functions import cache
from dsgrid.ibis.operations import filter_sql
from dsgrid.ibis.session import create_dataframe_from_dicts

from tests._helpers import collect as _collect, count as _count

STACKED_DIMENSION_COLUMNS = ["county", "model_year"]
PIVOTED_COLUMNS = ["elec_cooling", "elec_heating"]


@pytest.fixture
def datasets():
    df1 = create_dataframe_from_dicts(
        [
            {"county": "Jefferson", "model_year": "2030", "elec_cooling": 2, "elec_heating": 3},
            {"county": "Boulder", "model_year": "2030", "elec_cooling": 3, "elec_heating": 4},
            {"county": "Denver", "model_year": "2030", "elec_cooling": 5, "elec_heating": 6},
        ]
    )
    df2 = create_dataframe_from_dicts(
        [
            {"county": "Jefferson", "model_year": "2030", "elec_cooling": 9, "elec_heating": 10},
            {"county": "Boulder", "model_year": "2030", "elec_cooling": 10, "elec_heating": 11},
            {"county": "Denver", "model_year": "2030", "elec_cooling": 11, "elec_heating": 12},
        ]
    )
    dataset1 = DatasetExpressionHandler(df1, STACKED_DIMENSION_COLUMNS, PIVOTED_COLUMNS)
    dataset2 = DatasetExpressionHandler(df2, STACKED_DIMENSION_COLUMNS, PIVOTED_COLUMNS)
    yield {"dataset1": dataset1, "dataset2": dataset2}


def test_dataset_expression_add(datasets):
    df = evaluate_expression("dataset1 + dataset2", datasets).df
    cache(df)
    assert _count(df) == 3
    assert _collect(filter_sql(df, "county == 'Jefferson'"))[0].elec_cooling == 11
    assert _collect(filter_sql(df, "county == 'Boulder'"))[0].elec_cooling == 13
    assert _collect(filter_sql(df, "county == 'Denver'"))[0].elec_heating == 18
    assert df.columns == datasets["dataset1"].df.columns


def test_dataset_expression_mul(datasets):
    df = evaluate_expression("dataset1 * dataset2", datasets).df
    cache(df)
    assert _count(df) == 3
    assert _collect(filter_sql(df, "county == 'Jefferson'"))[0].elec_cooling == 18
    assert _collect(filter_sql(df, "county == 'Boulder'"))[0].elec_cooling == 30
    assert _collect(filter_sql(df, "county == 'Denver'"))[0].elec_heating == 72
    assert df.columns == datasets["dataset1"].df.columns


def test_dataset_expression_sub(datasets):
    df = evaluate_expression("dataset2 - dataset1", datasets).df
    cache(df)
    assert _count(df) == 3
    assert _collect(filter_sql(df, "county == 'Jefferson'"))[0].elec_cooling == 7
    assert _collect(filter_sql(df, "county == 'Boulder'"))[0].elec_cooling == 7
    assert _collect(filter_sql(df, "county == 'Denver'"))[0].elec_heating == 6
    assert df.columns == datasets["dataset1"].df.columns


def test_dataset_expression_union(datasets):
    df = evaluate_expression("dataset1 | dataset2", datasets).df
    cache(df)
    assert _count(df) == 6
    assert _count(filter_sql(df, "county == 'Jefferson'")) == 2
    assert _count(filter_sql(df, "county == 'Boulder'")) == 2
    assert _count(filter_sql(df, "county == 'Denver'")) == 2
    assert df.columns == datasets["dataset1"].df.columns


def test_dataset_expression_combo(datasets):
    df = evaluate_expression("(dataset1 + dataset2) | (dataset1 * dataset2)", datasets).df
    cache(df)
    assert _count(df) == 6
    jefferson = filter_sql(df, "county == 'Jefferson'")
    assert _count(jefferson) == 2
    assert sorted(x.elec_cooling for x in _collect(jefferson)) == [11, 18]
    boulder = filter_sql(df, "county == 'Boulder'")
    assert _count(boulder) == 2
    assert sorted(x.elec_cooling for x in _collect(boulder)) == [13, 30]
    denver = filter_sql(df, "county == 'Denver'")
    assert _count(denver) == 2
    assert sorted(x.elec_heating for x in _collect(denver)) == [18, 72]
    assert df.columns == datasets["dataset1"].df.columns


def test_invalid_lengths(datasets):
    # dataset3 = union of dataset1 and dataset2; both cover the same
    # dimension keys, so unioning produces duplicate-key rows. Multiplying
    # by dataset1 then multiplies each dataset1 row, which the post-join
    # row-count check catches as "multiplied rows" (duplicate keys on the
    # right side).
    datasets["dataset3"] = evaluate_expression("dataset1 | dataset2", datasets)
    with pytest.raises(DSGInvalidOperation, match="multiplied rows"):
        evaluate_expression("dataset1 * dataset3", datasets)


def test_invalid_join():
    # Make a county mismatch - Adams vs Jefferson - to trigger a join failure.
    df1 = create_dataframe_from_dicts(
        [
            {"county": "Adams", "model_year": "2030", "elec_cooling": 2, "elec_heating": 3},
            {"county": "Boulder", "model_year": "2030", "elec_cooling": 3, "elec_heating": 4},
            {"county": "Denver", "model_year": "2030", "elec_cooling": 5, "elec_heating": 6},
        ]
    )
    df2 = create_dataframe_from_dicts(
        [
            {"county": "Jefferson", "model_year": "2030", "elec_cooling": 9, "elec_heating": 10},
            {"county": "Boulder", "model_year": "2030", "elec_cooling": 10, "elec_heating": 11},
            {"county": "Denver", "model_year": "2030", "elec_cooling": 11, "elec_heating": 12},
        ]
    )
    dataset1 = DatasetExpressionHandler(df1, STACKED_DIMENSION_COLUMNS, PIVOTED_COLUMNS)
    dataset2 = DatasetExpressionHandler(df2, STACKED_DIMENSION_COLUMNS, PIVOTED_COLUMNS)
    datasets = {"dataset1": dataset1, "dataset2": dataset2}
    # dataset1 has Adams (not in dataset2) and dataset2 has Jefferson (not in
    # dataset1) — anti-joins on both sides catch the mismatched coverage.
    with pytest.raises(DSGInvalidOperation, match="mismatched dimension coverage"):
        evaluate_expression("dataset1 + dataset2", datasets)


def test_invalid_union():
    # Make a column mismatch to trigger an invalid union.
    df1 = create_dataframe_from_dicts(
        [
            {"county": "Adams", "model_year": "2030", "elec_cooling": 2},
            {"county": "Boulder", "model_year": "2030", "elec_cooling": 3},
            {"county": "Denver", "model_year": "2030", "elec_cooling": 5},
        ]
    )
    df2 = create_dataframe_from_dicts(
        [
            {"county": "Jefferson", "model_year": "2030", "elec_cooling": 9, "elec_heating": 10},
            {"county": "Boulder", "model_year": "2030", "elec_cooling": 10, "elec_heating": 11},
            {"county": "Denver", "model_year": "2030", "elec_cooling": 11, "elec_heating": 12},
        ]
    )
    dataset1 = DatasetExpressionHandler(df1, STACKED_DIMENSION_COLUMNS, PIVOTED_COLUMNS)
    dataset2 = DatasetExpressionHandler(df2, STACKED_DIMENSION_COLUMNS, PIVOTED_COLUMNS)
    datasets = {"dataset1": dataset1, "dataset2": dataset2}
    with pytest.raises(DSGInvalidOperation, match=r"Union.* datasets have identical columns"):
        evaluate_expression("dataset1 | dataset2", datasets)


def test_join_multiple_columns_direct():
    df1 = create_dataframe_from_dicts(
        [
            {"county": "Jefferson", "model_year": "2030", "elec_cooling": 2},
            {"county": "Boulder", "model_year": "2030", "elec_cooling": 3},
        ]
    )
    df2 = create_dataframe_from_dicts(
        [
            {"county": "Jefferson", "model_year": "2030", "elec_heating": 4},
            {"county": "Boulder", "model_year": "2030", "elec_heating": 5},
        ]
    )
    joined = join_multiple_columns(df1, df2, STACKED_DIMENSION_COLUMNS)
    rows = sorted(_collect(joined), key=lambda row: row.county)
    assert rows[0].county == "Boulder"
    assert rows[0].elec_cooling == 3
    assert rows[0].elec_heating == 5
    assert rows[1].county == "Jefferson"
    assert rows[1].elec_cooling == 2
    assert rows[1].elec_heating == 4
