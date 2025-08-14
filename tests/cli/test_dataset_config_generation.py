from click.testing import CliRunner

from dsgrid.cli.dsgrid import cli as cli
from dsgrid.config.dataset_config import DatasetConfigModel
from dsgrid.dimension.time import TimeDimensionType
from dsgrid.dimension.base_models import DimensionType
from dsgrid.tests.common import TEST_PROJECT_PATH
from dsgrid.utils.files import load_data
from common import check_config_fields


def test_generate_dataset_config_pivoted_matches(cached_registry, tmp_path):
    conn = cached_registry
    runner = CliRunner()
    dataset_id = "comstock"
    output_dir = tmp_path / dataset_id
    assert not output_dir.exists()
    cmd = [
        "--url",
        conn.url,
        "registry",
        "datasets",
        "generate-config",
        dataset_id,
        f"dsgrid-test-data/datasets/test_efs_{dataset_id}",
        "--project-id",
        "test_efs",
        "--schema-type",
        "standard",
        "--metric-type",
        "EnergyEndUse",
        "--pivoted-dimension-type",
        "metric",
        "--time-type",
        TimeDimensionType.DATETIME.value,
        "--time-columns",
        "timestamp",
        "--output",
        str(tmp_path),
        "--no-prompts",
    ]
    result = runner.invoke(cli, cmd)
    assert result.exit_code == 0
    assert output_dir.exists()
    dataset_file = output_dir / "dataset.json5"
    assert dataset_file.exists()
    dataset_config = load_data(dataset_file)
    dim_types = sorted([x["type"] for x in dataset_config["dimension_references"]])
    assert dim_types == [
        DimensionType.GEOGRAPHY.value,
        DimensionType.METRIC.value,
        DimensionType.SECTOR.value,
        DimensionType.SUBSECTOR.value,
    ]
    assert not list((output_dir / "dimensions").iterdir())


def test_generate_dataset_config_unpivoted_matches(cached_registry, tmp_path):
    conn = cached_registry
    runner = CliRunner()
    dataset_id = "comstock_unpivoted"
    output_dir = tmp_path / dataset_id
    assert not output_dir.exists()
    cmd = [
        "--url",
        conn.url,
        "registry",
        "datasets",
        "generate-config",
        dataset_id,
        f"dsgrid-test-data/datasets/test_efs_{dataset_id}",
        "--project-id",
        "test_efs",
        "--schema-type",
        "one_table",
        "--metric-type",
        "EnergyEndUse",
        "--output",
        str(tmp_path),
        "--no-prompts",
    ]
    result = runner.invoke(cli, cmd)
    assert result.exit_code == 0
    assert output_dir.exists()
    dataset_file = output_dir / "dataset.json5"
    assert dataset_file.exists()
    dataset_config = load_data(dataset_file)
    dim_types = sorted([x["type"] for x in dataset_config["dimension_references"]])
    assert dim_types == [
        DimensionType.GEOGRAPHY.value,
        DimensionType.METRIC.value,
        DimensionType.SECTOR.value,
        DimensionType.SUBSECTOR.value,
    ]
    assert not list((output_dir / "dimensions").iterdir())
    check_config_fields(output_dir / "dataset.json5", DatasetConfigModel)


def test_generate_dataset_config_partial_matches(cached_registry, tmp_path):
    conn = cached_registry
    runner = CliRunner()
    dataset_id = "comstock_conus_2022_projected"
    output_dir = tmp_path / dataset_id
    assert not output_dir.exists()
    dataset_path = (
        TEST_PROJECT_PATH
        / "filtered_registries"
        / "simple_standard_scenarios"
        / "data"
        / dataset_id
        / "1.0.0"
    )
    cmd = [
        "--url",
        conn.url,
        "registry",
        "datasets",
        "generate-config",
        dataset_id,
        str(dataset_path),
        "--project-id",
        "test_efs",
        "--schema-type",
        "one_table",
        "--metric-type",
        "EnergyEndUse",
        "--output",
        str(tmp_path),
        "--no-prompts",
    ]
    result = runner.invoke(cli, cmd)
    assert result.exit_code == 0
    assert output_dir.exists()
    dataset_file = output_dir / "dataset.json5"
    assert dataset_file.exists()
    dataset_config = load_data(dataset_file)
    dim_types = sorted([x["type"] for x in dataset_config["dimension_references"]])
    assert dim_types == [DimensionType.WEATHER_YEAR.value]
    assert sorted((x.name for x in (output_dir / "dimensions").iterdir())) == [
        DimensionType.GEOGRAPHY.value + ".csv",
        DimensionType.METRIC.value + ".csv",
        DimensionType.MODEL_YEAR.value + ".csv",
        DimensionType.SCENARIO.value + ".csv",
        DimensionType.SECTOR.value + ".csv",
        DimensionType.SUBSECTOR.value + ".csv",
    ]
    check_config_fields(output_dir / "dataset.json5", DatasetConfigModel)
