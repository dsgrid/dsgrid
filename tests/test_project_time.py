import logging
from pathlib import Path

import pyspark.sql.functions as F
import pytest

import pandas as pd
import numpy as np

from dsgrid.dimension.base_models import DimensionType
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.utils.spark import _get_spark_session

REGISTRY_PATH = (
    Path(__file__).absolute().parents[1]
    / "dsgrid-test-data"
    / "filtered_registries"
    / "simple_standard_scenarios"
)

logger = logging.getLogger(__name__)


@pytest.fixture
def registry_mgr():
    return RegistryManager.load(REGISTRY_PATH, offline_mode=True)


def test_convert_to_project_time(registry_mgr):
    project_id = "dsgrid_conus_2022"
    project = registry_mgr.project_manager.load_project(project_id)

    dataset_id = "conus_2022_reference_resstock"
    project.load_dataset(dataset_id)
    resstock = project.get_dataset(dataset_id)

    dataset_id = "conus_2022_reference_comstock"
    project.load_dataset(dataset_id)
    comstock = project.get_dataset(dataset_id)

    dataset_id = "tempo_conus_2022"
    project.load_dataset(dataset_id)
    tempo = project.get_dataset(dataset_id)

    # different ways to access project_time_dim:
    project_time_dim = project.config.get_base_dimension(
        DimensionType.TIME
    )  # or tempo._handler._project_time_dim
    resstock_time_dim = resstock._handler.config.get_dimension(DimensionType.TIME)
    comstock_time_dim = comstock._handler.config.get_dimension(DimensionType.TIME)
    tempo_time_dim = tempo._handler.config.get_dimension(DimensionType.TIME)

    # [1] test get_time_dataframe()
    check_time_dataframe(project_time_dim)
    check_time_dataframe(resstock_time_dim)
    check_time_dataframe(comstock_time_dim)
    tempo_time_dim.get_time_dataframe()
    # TODO: could add test for annual_time_dimension_config when AEO data is ready

    # [2] test convert_dataframe()
    # tempo time explosion
    # Test 1: feed in load_data and load_data_lookup separately
    tempo_time_dim.convert_dataframe(
        df=tempo.load_data,
        project_time_dim=project_time_dim,
        df_meta=tempo._handler._add_time_zone(tempo.load_data_lookup),
    )
    # Test 2: feed in load_data and lookup combined as tempo_data
    tempo_data = tempo._handler._add_time_zone(tempo.load_data_lookup)
    tempo_data = tempo.load_data.join(tempo_data, on="id")
    tempo_data = tempo_time_dim.convert_dataframe(
        df=tempo_data,
        project_time_dim=project_time_dim,
    )
    check_exploded_tempo_time(project_time_dim, tempo_data)

    # comstock time conversion
    comstock_data = comstock._handler._add_time_zone(comstock.load_data_lookup)
    comstock_data = comstock.load_data.join(comstock_data, on="id")
    comstock_data = comstock_time_dim.convert_dataframe(
        df=comstock_data,
        project_time_dim=project_time_dim,
    )
    # TODO: add test for annual_time_dimension_config, res/com with local timezones

    # [3] test make_project_dataframe()
    tempo._handler.make_project_dataframe()
    comstock._handler.make_project_dataframe()
    resstock._handler.make_project_dataframe()


def check_time_dataframe(time_dim):
    session_tz = _get_spark_session().conf.get("spark.sql.session.timeZone")
    time_df = time_dim.get_time_dataframe().toPandas()  # pyspark df
    time_range = time_dim.get_time_ranges()[0]

    time_df.iloc[:, 0] = time_df.iloc[:, 0].dt.tz_localize(session_tz)
    time_df_ts = time_df.iloc[0, 0]
    time_range_ts = time_range.start.tz_convert(session_tz)
    assert (
        time_df_ts == time_range_ts
    ), f"Starting timestamp does not match: {time_df_ts} vs. {time_range_ts}"

    time_df_ts = time_df.iloc[-1, 0]
    time_range_ts = time_range.end.tz_convert(session_tz)
    assert (
        time_df_ts == time_range_ts
    ), f"Ending timestamp does not match: {time_df_ts} vs. {time_range_ts}"


def check_exploded_tempo_time(project_time_dim, load_data):
    """
    - DF.show() (and probably all arithmetics) use spark session time
    - DF.toPandas(): likely go through spark session time
    - DF.collect() converts timestamps to system timezone (which is different than spark session timezone!)
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
    project_time = project_time_dim.get_time_dataframe()
    tempo_time = load_data.select(time_col).distinct().sort(F.asc(time_col))

    # QC 1: each timestamp has the same number of occurences
    freq_count = load_data.groupBy(time_col).count().select("count").distinct().collect()
    assert len(freq_count) == 1, freq_count

    # QC 2: model_time == project_time == tempo_time
    session_tz = _get_spark_session().conf.get("spark.sql.session.timeZone")
    model_time[time_col] = model_time[time_col].dt.tz_convert(session_tz)
    project_time = project_time.toPandas()
    project_time[time_col] = project_time[time_col].dt.tz_localize(session_tz)
    tempo_time = tempo_time.toPandas()
    tempo_time[time_col] = tempo_time[time_col].dt.tz_localize(session_tz)

    # Checks
    n_model = model_time[time_col].nunique()
    n_project = project_time[time_col].nunique()
    n_tempo = tempo_time[time_col].nunique()

    time = pd.concat(
        [
            model_time,
            project_time.rename(columns={time_col: "project_time"}),
            tempo_time.rename(columns={time_col: "tempo_time"}),
        ],
        axis=1,
    )

    mismatch = time[time.isna().any(axis=1)]
    print(time.iloc[7390:7400, :])
    assert n_model == 366 * 24, n_model
    assert (
        len(mismatch) == 0
    ), f"Mismatch:\nn_model={n_model}, n_project={n_project}, n_tempo={n_tempo}\n{mismatch}"
