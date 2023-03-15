import re
import shutil
from pathlib import Path

from dsgrid.registry.registry_database import DatabaseConnection
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.utils.run_command import check_run_command
from dsgrid.utils.files import load_data
from dsgrid.tests.common import TEST_DATASET_DIRECTORY
from dsgrid.tests.common import (
    map_dimension_names_to_ids,
    replace_dimension_names_with_current_ids,
)


def test_register_dimensions_and_mappings(tmp_registry_db):
    src_dir, tmpdir, db_name = tmp_registry_db
    check_run_command(f"dsgrid-admin create-registry {db_name} -p {tmpdir} --force")
    project_dimension_mapping_config = src_dir / "dimension_mappings_with_ids.json5"

    dim_config_file = src_dir / "dimensions.json5"
    cmd = f"dsgrid registry --offline --db-name {db_name} dimensions register {dim_config_file} -l log"
    check_run_command(cmd)
    conn = DatabaseConnection(database=db_name)
    manager = RegistryManager.load(conn, offline_mode=True)
    mappings = map_dimension_names_to_ids(manager.dimension_manager)
    replace_dimension_names_with_current_ids(project_dimension_mapping_config, mappings)

    # Registering duplicates is allowed.
    check_run_command(cmd)

    cmd = f"dsgrid registry --db-name={db_name} --offline dimension-mappings register {project_dimension_mapping_config} -l log"
    check_run_command(cmd)
    check_run_command(cmd)


def test_register_project_and_dataset(tmp_registry_db):
    src_dir, tmpdir, db_name = tmp_registry_db
    check_run_command(f"dsgrid-admin create-registry {db_name} -p {tmpdir} --force")
    dataset_dir = Path("datasets/modeled/comstock")

    def clear_spark_files():
        for path in (Path("spark-warehouse"), Path("metastore_db")):
            if path.exists():
                shutil.rmtree(path)

    project_config = src_dir / "project.json5"
    project_id = load_data(project_config)["project_id"]
    dataset_config = src_dir / dataset_dir / "dataset.json5"
    dataset_map_file = src_dir / dataset_dir / "dimension_mappings.json5"
    dataset_id = load_data(dataset_config)["dataset_id"]
    dataset_path = TEST_DATASET_DIRECTORY / dataset_id

    clear_spark_files()
    check_run_command(
        f"dsgrid registry --db-name={db_name} --offline projects register {project_config} "
        "--log-message log"
    )
    conn = DatabaseConnection(database=db_name)
    manager = RegistryManager.load(conn, offline_mode=True)
    mappings = map_dimension_names_to_ids(manager.dimension_manager)
    replace_dimension_names_with_current_ids(dataset_config, mappings)
    check_run_command(
        f"dsgrid registry --db-name={db_name} --offline projects register-and-submit-dataset "
        f"--dataset-config-file {dataset_config} "
        f"--dataset-path {dataset_path} "
        f"--dimension-mapping-file {dataset_map_file} "
        f"--project-id {project_id} "
        f"--log-message log"
    )
    output = {}

    clear_spark_files()
    check_run_command(f"dsgrid registry --db-name={db_name} --offline list", output)
    regex_project = re.compile(rf"{project_id}.*1\.1\.0")
    regex_dataset = re.compile(rf"{dataset_id}.*1\.0\.0")
    assert regex_project.search(output["stdout"]) is not None, output["stdout"]
    assert regex_dataset.search(output["stdout"]) is not None, output["stdout"]
    dim_id = manager.dimension_manager.list_ids()[0]
    dim_map_id = manager.dimension_mapping_manager.list_ids()[0]

    check_run_command(
        f"dsgrid-admin registry --db-name={db_name} --offline projects remove {project_id}"
    )
    check_run_command(
        f"dsgrid-admin registry --db-name={db_name} --offline datasets remove {dataset_id}"
    )
    check_run_command(
        f"dsgrid-admin registry --db-name={db_name} --offline dimension-mappings remove {dim_map_id}"
    )
    check_run_command(
        f"dsgrid-admin registry --db-name={db_name} --offline dimensions remove {dim_id}"
    )
