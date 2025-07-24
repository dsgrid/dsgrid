from click.testing import CliRunner

from dsgrid.cli.dsgrid import cli as cli
from dsgrid.config.project_config import ProjectConfigModel
from dsgrid.dimension.time import TimeDimensionType
from dsgrid.utils.files import load_data
from common import check_config_fields


def test_generate_project_config(cached_registry, tmp_path):
    conn = cached_registry
    runner = CliRunner()
    project_id = "my_project"
    dataset_ids = ["d1", "d2", "d3"]
    description = "This is a test project"
    output_dir = tmp_path / project_id
    assert not output_dir.exists()
    cmd = [
        "--url",
        conn.url,
        "registry",
        "projects",
        "generate-config",
        "--description",
        description,
        "--time-type",
        TimeDimensionType.DATETIME.value,
        "--metric-type",
        "EnergyEndUse",
        "--output",
        str(tmp_path),
        project_id,
        *dataset_ids,
    ]
    result = runner.invoke(cli, cmd)
    assert result.exit_code == 0
    assert output_dir.exists()
    project_file = output_dir / "project" / "project.json5"
    assert project_file.exists()
    config = load_data(project_file)
    assert [x["dataset_id"] for x in config["datasets"]] == dataset_ids
    assert config["project_id"] == project_id
    assert config["description"] == description
    assert config["name"] == ""
    assert (output_dir / "project" / "dimensions").exists()
    assert (output_dir / "project" / "dimension_mappings").exists()
    assert (output_dir / "datasets" / "historical").exists()
    assert (output_dir / "datasets" / "modeled").exists()
    check_config_fields(output_dir / "project" / "project.json5", ProjectConfigModel)
