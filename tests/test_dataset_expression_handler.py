import pytest
import ibis

from dsgrid.dataset.dataset_expression_handler import DatasetExpressionHandler, evaluate_expression
from dsgrid.exceptions import DSGInvalidOperation
from dsgrid.ibis.functions import cache
from dsgrid.ibis.operations import filter_sql
from dsgrid.ibis.session import create_dataframe_from_dicts

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
    datasets["dataset3"] = evaluate_expression("dataset1 | dataset2", datasets)
    with pytest.raises(DSGInvalidOperation, match="datasets have the same length"):
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
    with pytest.raises(DSGInvalidOperation, match="has a different row count"):
        evaluate_expression("dataset1 + dataset2", datasets)


def _count(df):
    if isinstance(df, ibis.Table):
        return df.count().execute()
    return df.count()


def _collect(df):
    if isinstance(df, ibis.Table):
        return list(df.execute().itertuples(index=False, name="Row"))
    return df.collect()


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
