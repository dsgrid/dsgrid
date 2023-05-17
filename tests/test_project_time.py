import logging

import pyspark.sql.functions as F
from pyspark.sql.types import FloatType
import pytest

import pandas as pd
import numpy as np
from zoneinfo import ZoneInfo

from dsgrid.dimension.base_models import DimensionType
from dsgrid.registry.registry_database import DatabaseConnection
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.dimension.time import TimeZone, TimeIntervalType
from dsgrid.utils.dataset import add_time_zone
from dsgrid.utils.spark import get_spark_session
from dsgrid.exceptions import DSGDatasetConfigError


logger = logging.getLogger(__name__)


@pytest.fixture
def registry_mgr():
    conn = DatabaseConnection(database="simple-standard-scenarios")
    return RegistryManager.load(conn, offline_mode=True)


def test_no_unexpected_timezone():
    for tzo in TimeZone:
        assert (
            tzo.is_standard() + tzo.is_prevailing() == 1
        ), f"{tzo} can either be prevailing or standard"


def test_convert_to_project_time(registry_mgr):
    project_id = "dsgrid_conus_2022"
    project = registry_mgr.project_manager.load_project(project_id)

    dataset_id = "resstock_conus_2022_reference"
    project.load_dataset(dataset_id)
    resstock = project.get_dataset(dataset_id)

    dataset_id = "comstock_conus_2022_reference"
    project.load_dataset(dataset_id)
    comstock = project.get_dataset(dataset_id)

    dataset_id = "tempo_conus_2022"
    project.load_dataset(dataset_id)
    tempo = project.get_dataset(dataset_id)
    tempo_load_data = tempo._handler._load_data
    tempo_load_data_lookup = tempo._handler._load_data_lookup

    # different ways to access project_time_dim:
    project_time_dim = project.config.get_base_dimension(
        DimensionType.TIME
    )  # or tempo._handler._project_time_dim
    resstock_time_dim = resstock._handler.config.get_dimension(DimensionType.TIME)
    comstock_time_dim = comstock._handler.config.get_dimension(DimensionType.TIME)
    comstock_geo_dim = comstock._handler.config.get_dimension(DimensionType.GEOGRAPHY)
    tempo_time_dim = tempo._handler.config.get_dimension(DimensionType.TIME)

    # [1] test build_time_dataframe()
    check_time_dataframe(project_time_dim)
    check_time_dataframe(resstock_time_dim)
    check_time_dataframe(comstock_time_dim)

    # [2] test convert time
    tempo_data = tempo_load_data.join(tempo_load_data_lookup, on="id").drop("id")
    tempo_data_mapped_time = tempo._handler._convert_time_dimension(tempo_data, project.config)
    tempo_data_with_tz = add_time_zone(
        tempo_data, project.config.get_base_dimension(DimensionType.GEOGRAPHY)
    )
    check_exploded_tempo_time(project_time_dim, tempo_data_mapped_time)
    check_tempo_load_sum(
        project_time_dim,
        tempo,
        raw_data=tempo_data_with_tz,
        converted_data=tempo_data_mapped_time,
    )
    compare_time_conversion(
        resstock_time_dim, project_time_dim, wrap_time=False, expect_error=True
    )
    compare_time_conversion(
        comstock_time_dim, project_time_dim, wrap_time=False, expect_error=True
    )
    compare_time_conversion(
        resstock_time_dim, project_time_dim, wrap_time=True, expect_error=False
    )
    compare_time_conversion(
        comstock_time_dim, project_time_dim, wrap_time=True, expect_error=False
    )
    compare_time_conversion(
        tempo_time_dim, project_time_dim, df=tempo_data_mapped_time, expect_error=False
    )

    # comstock time conversion
    comstock_data = comstock._handler._load_data.join(comstock._handler._load_data_lookup, on="id")
    comstock_data_with_tz = add_time_zone(comstock_data, comstock_geo_dim)
    comstock_time_dim.convert_dataframe(comstock_data_with_tz, project_time_dim)

    # [3] test make_project_dataframe()
    tempo._handler.make_project_dataframe(project.config)
    comstock._handler.make_project_dataframe(project.config)
    resstock._handler.make_project_dataframe(project.config)


def _compare_time_conversion(dataset_time_dim, project_time_dim, df=None, wrap_time=True):
    project_time = project_time_dim.build_time_dataframe()
    if df is None:
        converted_dataset_time = dataset_time_dim._convert_time_to_project_time_interval(
            dataset_time_dim.build_time_dataframe(), project_time_dim, wrap_time=wrap_time
        )
    else:
        converted_dataset_time = df
    ptime_col = project_time_dim.get_timestamp_load_data_columns()
    dfp = set(project_time.select(ptime_col).distinct().orderBy(ptime_col).collect())
    dfd = set(converted_dataset_time.select(ptime_col).distinct().orderBy(ptime_col).collect())

    if dfp != dfd:
        raise DSGDatasetConfigError(
            "dataset time dimension converted to project requirement does not match project time dimension. \n{delta}"
        )


def compare_time_conversion(
    dataset_time_dim, project_time_dim, df=None, wrap_time=True, expect_error=False
):
    if expect_error:
        with pytest.raises(DSGDatasetConfigError):
            _compare_time_conversion(
                dataset_time_dim, project_time_dim, df=df, wrap_time=wrap_time
            )
    else:
        _compare_time_conversion(dataset_time_dim, project_time_dim, df=df, wrap_time=wrap_time)


def check_time_dataframe(time_dim):
    session_tz = get_spark_session().conf.get("spark.sql.session.timeZone")
    time_df = time_dim.build_time_dataframe().collect()
    time_df_start = min(time_df)[0].astimezone(ZoneInfo(session_tz))
    time_df_end = max(time_df)[0].astimezone(ZoneInfo(session_tz))
    time_range = time_dim.get_time_ranges()[0]
    time_range_start = time_range.start.tz_convert(session_tz)
    time_range_end = time_range.end.tz_convert(session_tz)
    assert (
        time_df_start == time_range_start
    ), f"Starting timestamp does not match: {time_df_start} vs. {time_range_start}"
    assert (
        time_df_end == time_range_end
    ), f"Ending timestamp does not match: {time_df_end} vs. {time_range_end}"


def check_tempo_load_sum(project_time_dim, tempo, raw_data, converted_data):
    """check that annual sum from tempo data is the same when mapped in pyspark,
    and when mapped in pandas to get the frequency each value in raw_data gets mapped
    """
    spark = get_spark_session()
    session_tz_orig = session_tz = spark.conf.get("spark.sql.session.timeZone")

    ptime_col = project_time_dim.get_timestamp_load_data_columns()
    assert len(ptime_col) == 1, ptime_col
    ptime_col = ptime_col[0]

    tempo_time_dim = tempo._handler.config.get_dimension(DimensionType.TIME)
    time_cols = tempo_time_dim.get_timestamp_load_data_columns()
    enduse_cols = tempo._handler.get_pivoted_dimension_columns()

    # get sum from converted_data
    groupby_cols = [col for col in converted_data.columns if col not in enduse_cols + [ptime_col]]
    converted_sum = converted_data.groupBy(*groupby_cols).agg(
        *[F.sum(F.round(col, 3)).alias(col) for col in enduse_cols]
    )
    converted_sum_df = converted_sum.toPandas().set_index(groupby_cols).sort_index()

    # process raw_data, get freq each values will be mapped and get sumproduct from there
    # [1] sum from raw_data, mapping via pandas
    model_time = (
        pd.Series(np.concatenate(project_time_dim.list_expected_dataset_timestamps()))
        .rename(ptime_col)
        .to_frame()
    )
    model_time[ptime_col] = model_time[ptime_col].dt.tz_convert(session_tz)

    # convert to match time interval type
    dtime_int = tempo_time_dim.get_time_interval_type()
    ptime_int = project_time_dim.get_time_interval_type()
    match (ptime_int, dtime_int):
        case (TimeIntervalType.PERIOD_BEGINNING, TimeIntervalType.PERIOD_ENDING):
            model_time["map_time"] = model_time[ptime_col] + pd.Timedelta(
                project_time_dim.get_frequency()
            )

        case (TimeIntervalType.PERIOD_ENDING, TimeIntervalType.PERIOD_BEGINNING):
            model_time["map_time"] = model_time[ptime_col] - pd.Timedelta(
                project_time_dim.get_frequency()
            )

    geo_tz_values = [row.time_zone for row in raw_data.select("time_zone").distinct().collect()]
    geo_tz_names = [TimeZone(tz).tz_name for tz in geo_tz_values]

    model_time_df = []
    for tzv, tz in zip(geo_tz_values, geo_tz_names):
        model_time_tz = model_time.copy()
        model_time_tz["time_zone"] = tzv
        # for pd.dt.tz_convert(), always convert to UTC before converting to another tz
        model_time_tz["UTC"] = model_time_tz["map_time"].dt.tz_convert("UTC")
        model_time_tz["local_time"] = model_time_tz["UTC"].dt.tz_convert(tz)
        for col in time_cols:
            if col == "hour":
                model_time_tz[col] = model_time_tz["local_time"].dt.hour
            elif col == "day_of_week":
                model_time_tz[col] = model_time_tz["local_time"].dt.day_of_week
            elif col == "month":
                model_time_tz[col] = model_time_tz["local_time"].dt.month
            else:
                raise ValueError(f"{col} does not have a function specified in test.")
        model_time_df.append(model_time_tz)

    model_time_df = pd.concat(model_time_df, axis=0).reset_index(drop=True)
    model_time_map = (
        model_time_df.groupby(["time_zone"] + time_cols)[ptime_col]
        .count()
        .rename("count")
        .to_frame()
    )
    other_cols = [col for col in raw_data.columns if col not in enduse_cols]
    raw_data_df = (
        raw_data.select(other_cols + [F.round(col, 3).alias(col) for col in enduse_cols])
        .toPandas()
        .join(model_time_map, on=["time_zone"] + time_cols, how="left")
    )

    # [2] sum from raw_data, mapping via spark
    # temporarily set to UTC
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    session_tz = spark.conf.get("spark.sql.session.timeZone")

    try:
        project_time_df = project_time_dim.build_time_dataframe()
        project_time_df = project_time_dim._align_time_interval_type(
            project_time_df,
            ptime_col,
            ptime_int,
            dtime_int,
            project_time_dim.get_frequency(),
            new_time_column="map_time",
        )
        idx = 0
        for tz_value, tz_name in zip(geo_tz_values, geo_tz_names):
            local_time_df = (
                project_time_df.withColumn("time_zone", F.lit(tz_value))
                .withColumn("UTC", F.to_utc_timestamp(F.col("map_time"), session_tz))
                .withColumn("local_time", F.from_utc_timestamp(F.col("UTC"), tz_name))
            )
            select = [ptime_col, "map_time", "time_zone", "UTC", "local_time"]
            for col in time_cols:
                func = col.replace("_", "")
                expr = f"{func}(local_time) AS {col}"
                if col == "day_of_week":
                    expr = f"mod(dayofweek(local_time)+7-2, 7) AS {col}"
                select.append(expr)
            local_time_df = local_time_df.selectExpr(*select)
            if idx == 0:
                time_df = local_time_df
            else:
                time_df = time_df.union(local_time_df)
            idx += 1
    finally:
        # reset session timezone
        spark.conf.set("spark.sql.session.timeZone", session_tz_orig)
        session_tz = spark.conf.get("spark.sql.session.timeZone")

    raw_data_df2 = raw_data.join(
        time_df.groupBy(["time_zone"] + time_cols).count(),
        on=["time_zone"] + time_cols,
        how="left",
    )
    raw_sum_df2 = raw_data_df2.groupBy(groupby_cols).agg(
        *[
            F.sum(F.round(col, 3) * F.col("count").cast(FloatType())).alias(col)
            for col in enduse_cols
        ]
    )
    raw_sum_df2 = raw_sum_df2.toPandas().set_index(groupby_cols).sort_index()

    # check 1: that mapping df are the same for both spark and pandas
    time_df2 = time_df.collect()
    time_df2 = pd.DataFrame(time_df2, columns=time_df.columns)
    time_df2[ptime_col] = pd.to_datetime(time_df2[ptime_col]).dt.tz_localize(
        session_tz, ambiguous="infer"
    )

    cond = model_time_df["month"] != time_df2["month"]
    cond |= model_time_df["day_of_week"] != time_df2["day_of_week"]
    cond |= model_time_df["hour"] != time_df2["hour"]
    assert (
        len(time_df2[cond]) == 0
    ), f"Mismatch in mapping:\n{model_time_df[cond]}\n{time_df2[cond]}"

    # check 2: that the sum of frequency count is 8784 for both spark and pandas
    n_ts = raw_data_df.groupby(groupby_cols)["count"].sum().unique()
    assert list(n_ts) == [
        len(model_time)
    ], f"Mismatch in number of timestamps for pandas: {n_ts} vs. {len(model_time)}"
    n_ts2 = (
        raw_data_df2.groupBy(groupby_cols)
        .agg(F.sum("count").alias("count"))
        .select("count")
        .distinct()
        .toPandas()
    )
    assert n_ts2["count"].to_list() == [
        len(model_time)
    ], f"Mismatch in number of timestamps for spark: {n_ts2} vs. {len(model_time)}"

    # check 3: annual sum
    raw_data_df[enduse_cols] = raw_data_df[enduse_cols].multiply(raw_data_df["count"], axis=0)
    raw_sum_df = raw_data_df.groupby(groupby_cols)[enduse_cols].sum().sort_index()

    # compare annual sums
    delta_df = (converted_sum_df - raw_sum_df) / converted_sum_df
    delta_df[enduse_cols].abs().sum()

    delta_df2 = (converted_sum_df - raw_sum_df2) / converted_sum_df
    delta_df2[enduse_cols].abs().sum()

    # tolerance of 0.000 in pct change
    assert delta_df[enduse_cols].abs().sum().round(3).to_list() == [
        0
    ], f"Mismatch, delta:\n{delta_df[delta_df[enduse_cols]!=0]}"

    assert delta_df2[enduse_cols].abs().sum().round(3).to_list() == [
        0
    ], f"Mismatch, delta:\n{delta_df2[delta_df2[enduse_cols]!=0]}"


def check_exploded_tempo_time(project_time_dim, load_data):
    """
    - DF.show() (and probably all arithmetics) use spark.sql.session.timeZone
    - DF.toPandas() likely goes through spark.sql.session.timeZone
    - DF.collect() converts timestamps to system timezone (different from spark.sql.session.timeZone!)
    - hour(F.col(timestamp)) extracts hour from timestamp col as exactly shown in DF.show()
    - spark.sql.session.timeZone time that is consistent with system time seems to show time correctly
        (in session time) for DF.show(), however, it does not work well with time converting functions
        from spark.sql.functions
    - On the other hand, even though spark.sql.session.timeZone=UTC does not always show time correctly
        in DF.show(), it converts time correctly when using F.from_utc_timestamp() and F.to_utc_timestamp().
        Thus, we explicitly set session_tz to UTC when extracting timeinfo from local_time column.
    """

    # extract data for comparison
    time_col = project_time_dim.get_timestamp_load_data_columns()
    assert len(time_col) == 1, time_col
    time_col = time_col[0]

    model_time = (
        pd.Series(np.concatenate(project_time_dim.list_expected_dataset_timestamps()))
        .rename(time_col)
        .to_frame()
    )
    project_time = project_time_dim.build_time_dataframe()
    tempo_time = load_data.select(time_col).distinct().sort(F.asc(time_col))

    # QC 1: each timestamp has the same number of occurences
    freq_count = load_data.groupBy(time_col).count().select("count").distinct().collect()
    assert len(freq_count) == 1, freq_count

    # QC 2: model_time == project_time == tempo_time
    session_tz = get_spark_session().conf.get("spark.sql.session.timeZone")
    model_time[time_col] = model_time[time_col].dt.tz_convert(session_tz)
    project_time = [t[0].astimezone(ZoneInfo(session_tz)) for t in project_time.collect()]
    project_time = pd.DataFrame(project_time, columns=["project_time"])
    tempo_time = [t[0].astimezone(ZoneInfo(session_tz)) for t in tempo_time.collect()]
    tempo_time = pd.DataFrame(tempo_time, columns=["tempo_time"])

    # Checks
    n_model = model_time.iloc[:, 0].nunique()
    n_project = project_time.iloc[:, 0].nunique()
    n_tempo = tempo_time.iloc[:, 0].nunique()

    time = pd.concat(
        [
            model_time,
            project_time,
            tempo_time,
        ],
        axis=1,
    )

    mismatch = time[time.isna().any(axis=1)]
    assert n_model == 366 * 24, n_model
    assert (
        len(mismatch) == 0
    ), f"Mismatch:\nn_model={n_model}, n_project={n_project}, n_tempo={n_tempo}\n{mismatch}"
