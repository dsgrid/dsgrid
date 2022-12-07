import re
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory

from dsgrid.utils.run_command import check_run_command
from dsgrid.utils.files import load_data
from dsgrid.tests.common import TEST_DATASET_DIRECTORY
from dsgrid.tests.common import (
    replace_dimension_uuids_from_registry,
)


def test_register_dimensions_and_mappings(make_test_project_dir):
    with TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "registry"
        check_run_command(f"dsgrid-admin create-registry {path}")
        project_dimension_mapping_config = (
            make_test_project_dir / "dimension_mappings_with_ids.json5"
        )

        dim_config_file = make_test_project_dir / "dimensions.json5"
        cmd = (
            f"dsgrid registry --path={path} --offline dimensions register {dim_config_file} -l log"
        )
        check_run_command(cmd)
        replace_dimension_uuids_from_registry(path, (project_dimension_mapping_config,))

        # Registering duplicates is allowed.
        check_run_command(cmd)

        cmd = f"dsgrid registry --path={path} --offline dimension-mappings register {project_dimension_mapping_config} -l log"
        check_run_command(cmd)
        check_run_command(cmd)


def test_register_project_and_dataset(make_test_project_dir):
    with TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "registry"
        check_run_command(f"dsgrid-admin create-registry {path}")
        dataset_dir = Path("datasets/modeled/comstock")

        def clear_spark_files():
            for path in (Path("spark-warehouse"), Path("metastore_db")):
                if path.exists():
                    shutil.rmtree(path)

        src_dir = make_test_project_dir
        project_config = src_dir / "project.json5"
        project_id = load_data(project_config)["project_id"]
        dataset_config = src_dir / dataset_dir / "dataset.json5"
        dataset_map_file = src_dir / dataset_dir / "dimension_mappings.json5"
        dataset_id = load_data(dataset_config)["dataset_id"]
        dataset_path = TEST_DATASET_DIRECTORY / dataset_id

        clear_spark_files()
        check_run_command(
            f"dsgrid registry --path={path} --offline projects register {project_config} "
            "--log-message log"
        )
        replace_dimension_uuids_from_registry(path, (dataset_config,))
        check_run_command(
            f"dsgrid registry --path={path} --offline projects register-and-submit-dataset "
            f"--dataset-config-file {dataset_config} "
            f"--dataset-path {dataset_path} "
            f"--dimension-mapping-file {dataset_map_file} "
            f"--project-id {project_id} "
            f"--log-message log"
        )
        output = {}

        clear_spark_files()
        check_run_command(f"dsgrid registry --path={path} --offline list", output)
        regex_project = re.compile(rf"{project_id}.*1\.1\.0")
        regex_dataset = re.compile(rf"{dataset_id}.*1\.0\.0")
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
