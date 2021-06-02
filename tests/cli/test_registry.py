import re
from pathlib import Path
from tempfile import TemporaryDirectory, gettempdir


from dsgrid.utils.run_command import check_run_command, run_command
from dsgrid.tests.common import create_local_test_registry, make_test_project_dir
from tests.common import (
    replace_dimension_mapping_uuids_from_registry,
    replace_dimension_uuids_from_registry,
)


def test_register_project_and_dataset(make_test_project_dir):
    with TemporaryDirectory() as tmpdir:
        base_dir = Path(tmpdir)
        path = create_local_test_registry(base_dir)
        dataset_dir = Path("datasets/sector_models/comstock")
        dataset_dim_dir = dataset_dir / "dimensions"
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

        for dim_mapping_config in (project_dimension_mapping_config, dimension_mapping_config):
            cmd = f"dsgrid registry --path={path} --offline dimension-mappings register {dim_mapping_config} -l log"
            check_run_command(cmd)
            assert run_command(cmd) != 0

        project_config = make_test_project_dir / "project.toml"
        dataset_config = make_test_project_dir / dataset_dir / "dataset.toml"
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
            f"dsgrid registry --path={path} --offline projects submit-dataset -d efs_comstock -m {dimension_mapping_refs} -p test -l log"
        )
        output = {}
        check_run_command(f"dsgrid registry --path={path} --offline list", output)
        regex_project = re.compile(r"test.*1\.0\.0")
        regex_dataset = re.compile(r"efs_comstock.*1\.0\.0")
        assert regex_project.search(output["stdout"]) is not None
        assert regex_dataset.search(output["stdout"]) is not None
