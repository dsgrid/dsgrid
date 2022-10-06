import abc
import logging
import math
import shutil
import tempfile
from collections import defaultdict, namedtuple
from pathlib import Path
import shutil

import pyspark.sql.functions as F
import pytest
from pyspark.sql import SparkSession

import pandas as pd
import numpy as np

from dsgrid.dimension.base_models import DimensionType
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.dataset.dataset import Dataset
from dsgrid.project import Project
from dsgrid.dimension.time import TimeZone

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

    dataset_id = "tempo_conus_2022"
    project.load_dataset(dataset_id)
    tempo = project.get_dataset(dataset_id)

    dataset_id = "conus_2022_reference_comstock"
    project.load_dataset(dataset_id)
    comstock = project.get_dataset(dataset_id)

    # different ways to access project_time_dim:
    project_time_dim = project.config.get_base_dimension(DimensionType.TIME) # or tempo._handler._project_time_dim
    comstock_time_dim = comstock._handler.config.get_dimension(DimensionType.TIME)
    tempo_time_dim = tempo._handler.config.get_dimension(DimensionType.TIME)

    # [1] test get_time_dataframe()
    check_time_dataframe(project_time_dim)
    check_time_dataframe(comstock_time_dim)
    tempo_time_dim.get_time_dataframe()
    # TODO: could add test for annual_time_dimension_config when AEO data is ready


    # [2] test convert_dataframe()
    # tempo time explosion
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

    # [3] test make_project_dataframe()
    tempo._handler.make_project_dataframe()
    comstock._handler.make_project_dataframe()


def check_time_dataframe(time_dim):
    time_df = time_dim.get_time_dataframe().toPandas() # pyspark df
    time_range = time_dim.get_time_ranges()[0]

    time_df_ts = time_df.iloc[0,0]
    time_range_ts = time_range.start.tz_localize(None)
    assert time_df_ts == time_range_ts, f"Starting timestamp does not match: {time_df_ts} vs. {time_range_ts}"

    time_df_ts = time_df.iloc[-1,0]
    time_range_ts = time_range.end.tz_localize(None)
    assert time_df_ts == time_range_ts, f"Ending timestamp does not match: {time_df_ts} vs. {time_range_ts}"


def check_exploded_tempo_time(project_time_dim, load_data):
    """
    - DF.show() (and probably all arithmetics) use spark session time
    - DF.toPandas() has weird behavior (i.e., 2012-11-04 07:00 gets listed as 08:00, DThom is filing a bug report to spark)
    - DF.collect() converts timestamps to system timezone (which is different than spark session timezone!)
    - A safe solution is to:
        1) set session time to UTC,
        2) set system time to UTC (if that's doable, no solution yet)
        3) save DF to file and then reload
    """
    # extract data for comparison
    model_time = pd.Series(np.concatenate(project_time_dim.list_expected_dataset_timestamps())).rename("timestamp").to_frame()
    project_time = project_time_dim.get_time_dataframe()
    tempo_time = load_data.select("timestamp").distinct().sort(F.asc("timestamp"))

    # save to file and reload
    data_dir = Path(__file__).absolute().resolve().parents[0] / "data"
    data_dir.mkdir(exist_ok=True)
    project_time_file = data_dir / "temp-test_project_time-project_time.parquet"
    tempo_time_file = data_dir / "temp-test_project_time-tempo_time.parquet"
    if project_time_file.exists():
        shutil.rmtree(project_time_file)
    if tempo_time_file.exists():
        shutil.rmtree(tempo_time_file)
    project_time.write.parquet(str(project_time_file))
    tempo_time.write.parquet(str(tempo_time_file))
    project_time = pd.read_parquet(project_time_file)
    tempo_time = pd.read_parquet(tempo_time_file)

    model_time_tz = model_time["timestamp"].dt.tz
    project_time["timestamp"] = project_time["timestamp"].dt.tz_localize(model_time_tz)
    tempo_time["timestamp"] = tempo_time["timestamp"].dt.tz_localize(model_time_tz)

    # Checks
    n_model = model_time["timestamp"].nunique()
    n_project = project_time["timestamp"].nunique()
    n_tempo = tempo_time["timestamp"].nunique()

    time = model_time.join(
        project_time.assign(project_timestamp=project_time["timestamp"]).set_index("timestamp"),
        on="timestamp", how="outer").join(
        tempo_time.assign(tempo_timestamp=tempo_time["timestamp"]).set_index("timestamp"),
        on="timestamp", how="outer").reset_index(drop=True)

    mismatch = time[time.isna().any(axis=1)]
    assert n_model == 366*24, n_model
    assert len(mismatch) == 0, f"Mismatch:\nn_model={n_model}, n_project={n_project}, n_tempo={n_tempo}\n{mismatch}"

    # delete files
    shutil.rmtree(project_time_file)
    shutil.rmtree(tempo_time_file)
