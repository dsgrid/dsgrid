import logging

import pytest
from typing import Optional
from datetime import timedelta

import pandas as pd
from chronify.time_range_generator_factory import make_time_range_generator

from dsgrid.common import VALUE_COLUMN
from dsgrid.config.date_time_dimension_config import DateTimeDimensionConfig
from dsgrid.dataset.dataset_mapping_manager import DatasetMappingManager
from dsgrid.dimension.base_models import DimensionType
from dsgrid.dsgrid_rc import DsgridRuntimeConfig
from dsgrid.registry.registry_database import DatabaseConnection
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.dimension.time import TimeZone, TimeIntervalType, get_zone_info_from_spark_session

from dsgrid.spark.functions import (
    create_temp_view,
    make_temp_view_name,
    get_current_time_zone,
    join_multiple_columns,
    select_expr,
    set_current_time_zone,
    perform_interval_op,
)
from dsgrid.spark.types import (
    DataFrame,
    FloatType,
    F,
    StructField,
    StructType,
    TimestampType,
    use_duckdb,
)
from dsgrid.utils.dataset import add_time_zone
from dsgrid.utils.scratch_dir_context import ScratchDirContext
from dsgrid.utils.spark import get_spark_session, get_unique_values

from dsgrid.tests.common import SIMPLE_STANDARD_SCENARIOS_REGISTRY_DB


logger = logging.getLogger(__name__)


@pytest.fixture
def project():
    conn = DatabaseConnection(url=SIMPLE_STANDARD_SCENARIOS_REGISTRY_DB)
    with RegistryManager.load(conn, offline_mode=True) as registry_mgr:
        project_id = "dsgrid_conus_2022"
        project = registry_mgr.project_manager.load_project(project_id)
        yield project


@pytest.fixture
def resstock(project):
    dataset_id = "resstock_conus_2022_reference"
    project.load_dataset(dataset_id)
    return project.get_dataset(dataset_id)


@pytest.fixture
def resstock_inputdatasetmodel(project):
    dataset_id = "resstock_conus_2022_reference"
    return project.config.get_dataset(dataset_id)


@pytest.fixture
def comstock(project):
    dataset_id = "comstock_conus_2022_reference"
    project.load_dataset(dataset_id)
    return project.get_dataset(dataset_id)


@pytest.fixture
def tempo(project):
    dataset_id = "tempo_conus_2022"
    project.load_dataset(dataset_id)
    return project.get_dataset(dataset_id)


def test_no_unexpected_timezone():
    for tzo in TimeZone:
        if tzo != TimeZone.ARIZONA:
            assert (
                tzo.is_standard() + tzo.is_prevailing() == 1
            ), f"{tzo} can either be prevailing or standard"


def test_convert_time_for_tempo(project, tempo, scratch_dir_context):
    project_time_dim = project.config.get_base_dimension(DimensionType.TIME)

    tempo_data = tempo._handler._load_data.join(tempo._handler._load_data_lookup, on="id").drop(
        "id"
    )
    value_columns = tempo._handler.config.get_value_columns()
    assert len(value_columns) == 1
    value_column = next(iter(value_columns))
    plan = tempo._handler.build_default_dataset_mapping_plan()
    context = ScratchDirContext(DsgridRuntimeConfig.load().get_scratch_dir())
    input_dataset = project.config.get_dataset("tempo_conus_2022")
    with DatasetMappingManager(tempo._handler.dataset_id, plan, context) as mgr:
        tempo_data_mapped_time = tempo._handler._convert_time_dimension(
            tempo_data,
            to_time_dim=project_time_dim,
            mapping_manager=mgr,
            value_column=value_column,
            wrap_time_allowed=input_dataset.wrap_time_allowed,
            time_based_data_adjustment=input_dataset.time_based_data_adjustment,
            to_geo_dim=project.config.get_base_dimension(DimensionType.GEOGRAPHY),
        )
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


def shift_time_interval(
    df,
    time_column: str,
    from_time_interval: TimeIntervalType,
    to_time_interval: TimeIntervalType,
    time_step: timedelta,
    new_time_column: Optional[str] = None,
):
    """
    Shift time_column by time_step in df as needed by comparing from_time_interval
    to to_time_interval. If new_time_column is None, time_column is shifted in
    place, else shifted time is added as new_time_column in df.
    """
    assert (
        from_time_interval != to_time_interval
    ), f"{from_time_interval=} is the same as {to_time_interval=}"

    if new_time_column is None:
        new_time_column = time_column

    if TimeIntervalType.INSTANTANEOUS in (from_time_interval, to_time_interval):
        msg = "aligning time intervals with instantaneous is not yet supported"
        raise NotImplementedError(msg)

    match (from_time_interval, to_time_interval):
        case (TimeIntervalType.PERIOD_BEGINNING, TimeIntervalType.PERIOD_ENDING):
            df = perform_interval_op(
                df, time_column, "+", time_step.seconds, "SECONDS", new_time_column
            )
        case (TimeIntervalType.PERIOD_ENDING, TimeIntervalType.PERIOD_BEGINNING):
            df = perform_interval_op(
                df, time_column, "-", time_step.seconds, "SECONDS", new_time_column
            )

    return df


def check_tempo_load_sum(project_time_dim, tempo, raw_data, converted_data):
    """check that annual sum from tempo data is the same when mapped in pyspark,
    and when mapped in pandas to get the frequency each value in raw_data gets mapped
    """
    session_tz_orig = session_tz = get_current_time_zone()

    ptime_col = project_time_dim.get_load_data_time_columns()
    assert len(ptime_col) == 1, ptime_col
    ptime_col = ptime_col[0]

    tempo_time_dim = tempo._handler.config.get_dimension(DimensionType.TIME)
    time_cols = tempo_time_dim.get_load_data_time_columns()

    # get sum from converted_data
    groupby_cols = [col for col in converted_data.columns if col not in [ptime_col, VALUE_COLUMN]]
    converted_sum = converted_data.groupBy(*groupby_cols).agg(
        F.sum(VALUE_COLUMN).alias(VALUE_COLUMN)
    )
    pdf = converted_sum.toPandas()
    pdf[VALUE_COLUMN] = pdf[VALUE_COLUMN].round(3)
    converted_sum_df = pdf.set_index(groupby_cols).sort_index()

    # process raw_data, get freq each values will be mapped and get sumproduct from there
    # [1] sum from raw_data, mapping via pandas
    project_time_df, project_timestamps = make_date_time_df(project_time_dim)
    model_time = pd.Series(project_timestamps).rename(ptime_col).to_frame()
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

    geo_tz_names = sorted(get_unique_values(raw_data, "time_zone"))
    assert geo_tz_names

    model_time_df = []
    for tz_name in geo_tz_names:
        model_time_tz = model_time.copy()
        model_time_tz["time_zone"] = tz_name
        # for pd.dt.tz_convert(), always convert to UTC before converting to another tz
        model_time_tz["UTC"] = model_time_tz["map_time"].dt.tz_convert("UTC")
        model_time_tz["local_time"] = model_time_tz["UTC"].dt.tz_convert(tz_name)
        for col in time_cols:
            if col == "hour":
                model_time_tz[col] = model_time_tz["local_time"].dt.hour
            elif col == "day_of_week":
                model_time_tz[col] = model_time_tz["local_time"].dt.day_of_week
            elif col == "month":
                model_time_tz[col] = model_time_tz["local_time"].dt.month
            else:
                msg = f"{col} does not have a function specified in test."
                raise ValueError(msg)
        model_time_df.append(model_time_tz)

    model_time_df = pd.concat(model_time_df, axis=0).reset_index(drop=True)
    model_time_map = (
        model_time_df.groupby(["time_zone"] + time_cols)[ptime_col]
        .count()
        .rename("count")
        .to_frame()
    )
    other_cols = [col for col in raw_data.columns if col != VALUE_COLUMN]
    raw_data_df = (
        raw_data.select(other_cols + [VALUE_COLUMN])
        .toPandas()
        .join(model_time_map, on=["time_zone"] + time_cols, how="left")
    )
    raw_data_df[VALUE_COLUMN] = raw_data_df[VALUE_COLUMN].round(3)

    # [2] sum from raw_data, mapping via spark
    # temporarily set to UTC
    set_current_time_zone("UTC")
    session_tz = get_current_time_zone()

    try:
        project_time_df = shift_time_interval(
            project_time_df,
            ptime_col,
            ptime_int,
            dtime_int,
            project_time_dim.get_frequency(),
            new_time_column="map_time",
        )
        idx = 0
        if use_duckdb():
            weekday_func = "ISODOW"
            weekday_modifier = " - 1"
        else:
            weekday_func = "WEEKDAY"
            weekday_modifier = ""
        for tz_name in geo_tz_names:
            local_time_df = project_time_df.withColumn("time_zone", F.lit(tz_name))
            local_time_df = to_utc_timestamp(local_time_df, "map_time", session_tz, "UTC")
            local_time_df = from_utc_timestamp(local_time_df, "UTC", tz_name, "local_time")
            select = [ptime_col, "map_time", "time_zone", "UTC", "local_time"]
            for col in time_cols:
                func = col.replace("_", "")
                expr = f"{func}(local_time) AS {col}"
                if col == "day_of_week":
                    expr = f"{weekday_func}(local_time) {weekday_modifier} AS {col}"
                select.append(expr)
            local_time_df = select_expr(local_time_df, select)
            if idx == 0:
                time_df = local_time_df
            else:
                time_df = time_df.union(local_time_df)
            idx += 1
        assert isinstance(time_df, DataFrame)
        if use_duckdb():
            # DuckDB does not persist the hour value unless we create a table.
            view = create_temp_view(time_df)
            table = make_temp_view_name()
            spark = get_spark_session()
            spark.sql(f"CREATE TABLE {table} AS SELECT * FROM {view}")
            time_df = spark.sql(f"SELECT * FROM {table}")
    finally:
        # reset session timezone
        set_current_time_zone(session_tz_orig)
        session_tz = get_current_time_zone()

    grouped_time_df = time_df.groupBy(["time_zone"] + time_cols).count()

    raw_data_df2 = join_multiple_columns(
        raw_data,
        grouped_time_df,
        ["time_zone"] + time_cols,
        how="left",
    )

    raw_sum_df2 = raw_data_df2.groupBy(groupby_cols).agg(
        F.sum(F.col(VALUE_COLUMN) * F.col("count").cast(FloatType())).alias(VALUE_COLUMN)
    )
    raw_sum_df2 = raw_sum_df2.toPandas().set_index(groupby_cols).sort_index()
    raw_sum_df2[VALUE_COLUMN] = raw_sum_df2[VALUE_COLUMN].round(3)

    # check 1: that mapping df are the same for both spark and pandas
    time_df2 = time_df.toPandas()
    if not use_duckdb():
        time_df2[ptime_col] = pd.to_datetime(time_df2[ptime_col]).dt.tz_localize(
            session_tz, ambiguous="infer"
        )
    # else: already tz_aware?

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
    raw_data_df[VALUE_COLUMN] = raw_data_df[VALUE_COLUMN].multiply(raw_data_df["count"], axis=0)
    raw_sum_df = raw_data_df.groupby(groupby_cols)[[VALUE_COLUMN]].sum().sort_index()

    # compare annual sums
    delta_df = (converted_sum_df - raw_sum_df) / converted_sum_df
    delta_df2 = (converted_sum_df - raw_sum_df2) / converted_sum_df

    # tolerance of 0.000 in pct change
    assert delta_df[VALUE_COLUMN].abs().sum().round(3) == 0.0
    assert delta_df2[VALUE_COLUMN].abs().sum().round(3) == 0.0


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
    time_col = project_time_dim.get_load_data_time_columns()
    assert len(time_col) == 1, time_col
    time_col = time_col[0]

    project_time, project_timestamps = make_date_time_df(project_time_dim)
    model_time = pd.Series(project_timestamps).rename(time_col).to_frame()
    tempo_time = load_data.select(time_col).distinct().sort(time_col)

    # QC 1: each timestamp has the same number of occurences
    freq_count = load_data.groupBy(time_col).count().select("count").distinct().collect()
    assert len(freq_count) == 1, freq_count

    # QC 2: model_time == project_time == tempo_time
    session_tz = get_current_time_zone()
    assert session_tz is not None
    z_info = get_zone_info_from_spark_session(session_tz)
    model_time[time_col] = model_time[time_col].dt.tz_convert(session_tz)
    project_time = [t[0].astimezone(z_info) for t in project_time.collect()]
    project_time = pd.DataFrame(project_time, columns=["project_time"])
    tempo_time = [t[0].astimezone(z_info) for t in tempo_time.collect()]
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


# The duckdb implementations of the next two functions may not be fully correct or ideal.
# We don't need them in the dsgrid package because time mapping is performed in chronify.
# There are some extensive tests in this file that rely on them, and so we are keeping them
# here.


def from_utc_timestamp(
    df: DataFrame, time_column: str, time_zone: str, new_column: str
) -> DataFrame:
    """Refer to pyspark.sql.functions.from_utc_timestamp."""
    if use_duckdb():
        view = create_temp_view(df)
        cols = df.columns[:]
        if time_column == new_column:
            cols.remove(time_column)
        cols_str = ",".join(cols)
        query = f"""
            SELECT
                {cols_str},
                CAST(timezone('{time_zone}', {time_column}) AS TIMESTAMPTZ) AS {new_column}
            FROM {view}
        """
        df2 = get_spark_session().sql(query)
        return df2

    df2 = df.withColumn(new_column, F.from_utc_timestamp(time_column, time_zone))
    return df2


def to_utc_timestamp(
    df: DataFrame, time_column: str, time_zone: str, new_column: str
) -> DataFrame:
    """Refer to pyspark.sql.functions.to_utc_timestamp."""
    if use_duckdb():
        view = create_temp_view(df)
        cols = df.columns[:]
        if time_column == new_column:
            cols.remove(time_column)
        cols_str = ",".join(cols)
        query = f"""
            SELECT
                {cols_str},
                CAST(timezone('{time_zone}', {time_column}) AS TIMESTAMPTZ) AS {new_column}
            FROM {view}
        """
        df2 = get_spark_session().sql(query)
        return df2

    df2 = df.withColumn(new_column, F.to_utc_timestamp(time_column, time_zone))
    return df2


def make_date_time_df(
    time_config: DateTimeDimensionConfig,
) -> tuple[DataFrame, list[pd.Timestamp]]:
    timestamps = make_time_range_generator(time_config.to_chronify()).list_timestamps()
    project_time_cols = time_config.get_load_data_time_columns()
    assert len(project_time_cols) == 1, project_time_cols
    time_col = project_time_cols[0]
    schema = StructType([StructField(time_col, TimestampType(), False)])
    df = get_spark_session().createDataFrame(
        [(x.to_pydatetime(),) for x in timestamps], schema=schema
    )
    return df, timestamps
