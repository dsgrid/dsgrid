import re
from pathlib import Path

from click.testing import CliRunner

from dsgrid.cli.dsgrid import cli
from dsgrid.config.registration_models import RegistrationModel
from dsgrid.query.models import ColumnType
from dsgrid.registry.common import DatabaseConnection
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.tests.common import (
    IEF_PROJECT_REPO,
    STANDARD_SCENARIOS_PROJECT_REPO,
    TEST_EFS_REGISTRATION_FILE,
    TEST_PROJECT_PATH,
)
from dsgrid.utils.files import dump_data, load_data
from dsgrid.utils.scratch_dir_context import ScratchDirContext
from dsgrid.utils.id_remappings import (
    map_dimension_names_to_ids,
    replace_dimension_names_with_current_ids,
)


def test_register_dimensions_and_mappings(tmp_registry_db):
    src_dir, tmpdir, url = tmp_registry_db
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "registry",
            "create",
            url,
            "-p",
            str(tmpdir),
            "--force",
        ],
    )
    assert result.exit_code == 0
    project_dimension_mapping_config = src_dir / "dimension_mappings_with_ids.json5"

    dim_config_file = src_dir / "dimensions.json5"
    cmd = [
        "--url",
        url,
        "registry",
        "dimensions",
        "register",
        str(dim_config_file),
        "-l",
        "log",
    ]
    result = runner.invoke(cli, cmd)
    assert result.exit_code == 0
    conn = DatabaseConnection(url=url)
    with RegistryManager.load(conn, offline_mode=True) as manager:
        mappings = map_dimension_names_to_ids(manager.dimension_manager)
        replace_dimension_names_with_current_ids(project_dimension_mapping_config, mappings)

    # Registering duplicates is allowed.
    result = runner.invoke(cli, cmd)
    assert result.exit_code == 0

    cmd = [
        "--url",
        url,
        "registry",
        "dimension-mappings",
        "register",
        str(project_dimension_mapping_config),
        "-l",
        "log",
    ]
    result = runner.invoke(cli, cmd)
    assert result.exit_code == 0
    result = runner.invoke(cli, cmd)
    assert result.exit_code == 0


def test_register_project_and_dataset(tmp_registry_db):
    src_dir, tmpdir, url = tmp_registry_db
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "registry",
            "create",
            url,
            "-p",
            str(tmpdir),
            "--force",
        ],
    )
    assert result.exit_code == 0
    dataset_dir = Path("datasets/modeled/comstock")

    project_config = src_dir / "project.json5"
    project_id = load_data(project_config)["project_id"]
    dataset_config = src_dir / dataset_dir / "dataset.json5"
    dataset_map_file = src_dir / dataset_dir / "dimension_mappings.json5"
    dataset_id = load_data(dataset_config)["dataset_id"]

    result = runner.invoke(
        cli,
        [
            "--url",
            url,
            "registry",
            "projects",
            "register",
            str(project_config),
            "--log-message",
            "log",
        ],
    )
    assert result.exit_code == 0
    conn = DatabaseConnection(url=url)
    with RegistryManager.load(conn, offline_mode=True) as manager:
        mappings = map_dimension_names_to_ids(manager.dimension_manager)
        replace_dimension_names_with_current_ids(dataset_config, mappings)
    cmd = [
        "--url",
        url,
        "registry",
        "projects",
        "register-and-submit-dataset",
        "--dataset-config-file",
        str(dataset_config),
        "--dimension-mapping-file",
        str(dataset_map_file),
        "--project-id",
        project_id,
        "--log-message",
        "log",
    ]

    result = runner.invoke(cli, cmd)
    assert result.exit_code == 0, result.output

    result = runner.invoke(cli, ["--url", url, "registry", "list"])
    assert result.exit_code == 0
    regex_project = re.compile(rf"{project_id}.*1\.1\.0")
    regex_dataset = re.compile(rf"{dataset_id}.*1\.0\.0")
    assert regex_project.search(result.stdout) is not None, result.stdout
    assert regex_dataset.search(result.stdout) is not None, result.stdout
    with RegistryManager.load(conn, offline_mode=True) as manager:
        dim_id = manager.dimension_manager.list_ids()[0]
        dim_map_id = manager.dimension_mapping_manager.list_ids()[0]

    result = runner.invoke(
        cli,
        [
            "--url",
            url,
            "registry",
            "projects",
            "remove",
            project_id,
        ],
    )
    assert result.exit_code == 0
    result = runner.invoke(
        cli,
        [
            "--url",
            url,
            "registry",
            "datasets",
            "remove",
            dataset_id,
        ],
    )
    assert result.exit_code == 0
    result = runner.invoke(
        cli,
        [
            "--url",
            url,
            "registry",
            "dimension-mappings",
            "remove",
            dim_map_id,
        ],
    )
    assert result.exit_code == 0
    result = runner.invoke(
        cli,
        [
            "--url",
            url,
            "registry",
            "dimensions",
            "remove",
            dim_id,
        ],
    )
    assert result.exit_code == 0


def test_list_project_dimension_names(cached_registry):
    conn = cached_registry
    runner = CliRunner()
    cmd = [
        "--url",
        conn.url,
        "registry",
        "projects",
        "list-dimension-names",
        "test_efs",
    ]
    result = runner.invoke(cli, cmd)
    assert result.exit_code == 0
    # Line breaks are inexplicably sometimes present in single lines, only in tests.
    normalized_output = " ".join(result.stdout.split())
    assert "base: US Counties" in normalized_output
    assert "subset: commercial_subsectors2 residential_subsectors" in normalized_output
    assert (
        "supplemental: Commercial Subsectors Subsectors by Sector all_test_efs_subsectors"
        in normalized_output
    )
    assert "supplemental: US Census Divisions US Census Regions US States" in normalized_output


def test_register_dsgrid_projects(tmp_registry_db):
    """Test registration of the real dsgrid projects."""
    _, tmpdir, url = tmp_registry_db
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "registry",
            "create",
            url,
            "-p",
            str(tmpdir),
            "--force",
        ],
    )
    assert result.exit_code == 0

    project_configs = (
        STANDARD_SCENARIOS_PROJECT_REPO / "dsgrid_project" / "project.json5",
        IEF_PROJECT_REPO / "project" / "project.json5",
    )

    # Test these together because they share dimensions and mappings.
    for project_config in project_configs:
        result = runner.invoke(
            cli,
            [
                "--url",
                url,
                "registry",
                "projects",
                "register",
                str(project_config),
                "--log-message",
                f"Register project {project_config}",
            ],
        )
        assert result.exit_code == 0

    conn = DatabaseConnection(url=url)
    with RegistryManager.load(conn, offline_mode=True) as manager:
        project = manager.project_manager.load_project("US_DOE_IEF_2025")
        config = project.config
        context = ScratchDirContext(tmpdir)
        config.make_dimension_association_table("ief_2025_transport", context)


def test_bulk_register(tmp_registry_db):
    test_project_dir, tmp_path, url = tmp_registry_db
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "registry",
            "create",
            url,
            "-p",
            str(tmp_path),
            "--force",
        ],
    )
    assert result.exit_code == 0

    # Inject an error so that registration of the second dataset fails.
    dataset_config_file = test_project_dir / "datasets" / "modeled" / "comstock" / "dataset.json5"
    config = load_data(dataset_config_file)
    config["dimensions"][0]["name"] += "!@#$%"
    dump_data(config, dataset_config_file)
    registration = RegistrationModel.from_file(TEST_EFS_REGISTRATION_FILE)
    registration.datasets[1].config_file = dataset_config_file
    registration_file = tmp_path / "registration.json5"
    registration_file.write_text(registration.model_dump_json(indent=2), encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--url",
            url,
            "registry",
            "bulk-register",
            str(registration_file),
        ],
    )
    assert result.exit_code != 0
    regex = re.compile(
        r"Recorded successfully registered projects and datasets to ([\w-]+\.json5)"
    )
    match = regex.search(result.stderr)
    assert match
    journal_file = Path(match.group(1))
    assert journal_file.exists()

    result = runner.invoke(
        cli,
        [
            "--url",
            url,
            "registry",
            "bulk-register",
            str(TEST_EFS_REGISTRATION_FILE),
            "-j",
            str(journal_file),
        ],
    )
    assert result.exit_code == 0
    assert not journal_file.exists()


def test_register_multiple_metric_dimensions(tmp_registry_db):
    _, tmpdir, url = tmp_registry_db
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "registry",
            "create",
            url,
            "-p",
            str(tmpdir),
            "--force",
        ],
    )
    assert result.exit_code == 0
    base = TEST_PROJECT_PATH / "filtered_registries" / "dgen_multiple_metrics"
    project_dir = base / "dsgrid-project-IEF"
    project_config_file = project_dir / "project" / "project.json5"
    profiles_config_file = project_dir / "datasets" / "modeled" / "dgen_profiles" / "dataset.json5"
    profiles_mapping_file = (
        project_dir / "datasets" / "modeled" / "dgen_profiles" / "dimension_mappings.json5"
    )
    capacities_config_file = (
        project_dir / "datasets" / "modeled" / "dgen_capacities" / "dataset.json5"
    )
    capacities_mapping_file = (
        project_dir / "datasets" / "modeled" / "dgen_capacities" / "dimension_mappings.json5"
    )
    cmd = [
        "--url",
        url,
        "registry",
        "projects",
        "register",
        str(project_config_file),
        "-l",
        "log",
    ]
    result = runner.invoke(cli, cmd)
    assert result.exit_code == 0

    cmds = (
        [
            "-u",
            url,
            "registry",
            "projects",
            "register-and-submit-dataset",
            "-c",
            str(profiles_config_file),
            "-p",
            "US_DOE_DECARB_2023",
            "-m",
            str(profiles_mapping_file),
            "-l",
            "Register and submit dgen profiles",
        ],
        [
            "-u",
            url,
            "registry",
            "projects",
            "register-and-submit-dataset",
            "-c",
            str(capacities_config_file),
            "-p",
            "US_DOE_DECARB_2023",
            "-m",
            str(capacities_mapping_file),
            "-l",
            "Register and submit dgen capacities",
        ],
    )
    for cmd in cmds:
        result = runner.invoke(cli, cmd)
        assert result.exit_code == 0

    for dataset_id in ("decarb_2023_dgen", "decarb_2023_dgen_capacities"):
        for column_type in ColumnType:
            cmd = [
                "-u",
                url,
                "query",
                "project",
                "map-dataset",
                "US_DOE_DECARB_2023",
                dataset_id,
                "-o",
                str(tmpdir),
                "--column-type",
                column_type.value,
                "--overwrite",
            ]
            result = runner.invoke(cli, cmd)
            assert result.exit_code == 0
            match = re.search(r"Wrote query.*to (.*parquet)", result.stderr)
            assert match
            path = Path(match.group(1))
            assert path.exists()
        # Correctness of map_dataset is tested elsewhere.
