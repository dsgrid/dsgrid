import os
import re
from pathlib import Path
from tempfile import TemporaryDirectory, gettempdir


from dsgrid.utils.run_command import check_run_command, run_command
from dsgrid.utils.files import load_data
from dsgrid.tests.common import (
    create_local_test_registry,
    make_test_project_dir,
    TEST_DATASET_DIRECTORY,
)
from dsgrid.tests.common import (
    replace_dimension_mapping_uuids_from_registry,
    replace_dimension_uuids_from_registry,
)
from tests.make_us_data_registry import replace_dataset_path


def test_register_project_and_dataset(make_test_project_dir):
    with TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "registry"
        check_run_command(f"dsgrid-admin create-registry {path}")
        dataset_dir = Path("datasets/sector_models/comstock")
        project_dimension_mapping_config = make_test_project_dir / "dimension_mappings.toml"
        dimension_mapping_config = make_test_project_dir / dataset_dir / "dimension_mappings.toml"
        dimension_mapping_refs = (
            make_test_project_dir / dataset_dir / "dimension_mapping_references.toml"
        )

        for dim_config_file in (
            make_test_project_dir / "dimensions.toml",
            make_test_project_dir / dataset_dir / "dimensions.toml",
        ):
            cmd = f"dsgrid registry --path={path} --offline dimensions register {dim_config_file} -l log"
            check_run_command(cmd)
            # Can't register duplicates.
            if dim_config_file == make_test_project_dir / "dimensions.toml":
                # The other one has time only - no records.
                assert run_command(cmd) != 0

        replace_dimension_uuids_from_registry(
            path, (project_dimension_mapping_config, dimension_mapping_config)
        )

        for dim_mapping_config in (project_dimension_mapping_config, dimension_mapping_config):
            cmd = f"dsgrid registry --path={path} --offline dimension-mappings register {dim_mapping_config} -l log"
            check_run_command(cmd)
            assert run_command(cmd) != 0

        project_config = make_test_project_dir / "project.toml"
        project_id = load_data(project_config)["project_id"]
        dataset_config = make_test_project_dir / dataset_dir / "dataset.toml"
        dataset_id = load_data(dataset_config)["dataset_id"]
        replace_dataset_path(dataset_config, TEST_DATASET_DIRECTORY)
        replace_dimension_mapping_uuids_from_registry(
            path, (project_config, dimension_mapping_refs)
        )
        replace_dimension_uuids_from_registry(path, (project_config, dataset_config))

        check_run_command(
            f"dsgrid registry --path={path} --offline datasets register {dataset_config} -l log"
        )
        check_run_command(
            f"dsgrid registry --path={path} --offline projects register {project_config} -l log"
        )
        check_run_command(
            f"dsgrid registry --path={path} --offline projects submit-dataset -d {dataset_id} "
            f"-m {dimension_mapping_refs} -p {project_id} -l log"
        )
        output = {}
        check_run_command(f"dsgrid registry --path={path} --offline list", output)
        regex_project = re.compile(fr"{project_id}.*1\.1\.0")
        regex_dataset = re.compile(fr"{dataset_id}.*1\.0\.0")
        assert regex_project.search(output["stdout"]) is not None, output["stdout"]
        assert regex_dataset.search(output["stdout"]) is not None, output["stdout"]
        dim_map_id = next((path / "configs" / "dimension_mappings").iterdir()).name
        dim_id = next((path / "configs" / "dimensions" / "geography").iterdir()).name

        check_run_command(
            f"dsgrid-admin registry --path={path} --offline projects remove {project_id}"
        )
        check_run_command(
            f"dsgrid-admin registry --path={path} --offline datasets remove {dataset_id}"
        )
        check_run_command(
            f"dsgrid-admin registry --path={path} --offline dimension-mappings remove {dim_map_id}"
        )
        check_run_command(
            f"dsgrid-admin registry --path={path} --offline dimensions remove {dim_id}"
        )
