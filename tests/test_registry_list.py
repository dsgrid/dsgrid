import pytest
from click.testing import CliRunner

import dsgrid.cli.registry
from dsgrid.cli.dsgrid import cli
from dsgrid.common import REMOTE_REGISTRY
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.exceptions import DSGInvalidParameter


@pytest.fixture(scope="session")
def load_registry_manager(cached_registry):
    conn = cached_registry
    with RegistryManager.load(
        conn,
        remote_path=REMOTE_REGISTRY,
        offline_mode=True,
        no_prompts=True,
    ) as manager:
        yield conn, manager


def test_registry_list_all(load_registry_manager):
    """
    test:
    dsgrid registry sync (not included, add?)
    dsgrid registry list
    dsgrid registry projects|datasets|dimension|dimension-mappings list
    """
    registry_manager = load_registry_manager[1]
    registry_manager.show()
    registry_manager.project_manager.show()
    registry_manager.dataset_manager.show()
    registry_manager.dimension_manager.show()
    registry_manager.dimension_mapping_manager.show()


def test_registry_list_filters(load_registry_manager):
    """
    test different variation of:
    dsgrid registry projects|datasets|dimensions|dimension-mappings list -f cond1 -f cond2
    """
    registry_manager = load_registry_manager[1]
    filters1 = ("submitter!=mn",)  # works for all
    filters2 = ("id contains geography", "registration date not contains -06-01")  # works for all

    filters3 = ("type==time", "description contains period-ending")  # works for dimension only
    filters4 = (
        "type [from, to] not contains time",
        "description contains comstock",
    )  # works for dimension_mapping only

    filters5 = ("submitter == mmooney", "registration contains 2021")  # wrong field in 2nd cond
    filters6 = ("submitter = mmooney", "registration date contains 2021")  # wrong op in 1st cond
    filters7 = (
        "type != subsector",
        "version not contains 2.0.0",
    )  # field in 1st cond not available in most managers

    # test set 1: good filters for all managers
    good_filters = [filters1, filters2]
    for filters in good_filters:
        registry_manager.show(filters=filters)

    # test set 2: good filters for selected managers
    registry_manager.dimension_manager.show(filters=filters3)
    registry_manager.dimension_mapping_manager.show(filters=filters4)

    # test set 3: test bad filters with exceptions
    bad_filters = [filters5, filters6, filters7]
    with pytest.raises(DSGInvalidParameter):
        for filters in bad_filters:
            registry_manager.project_manager.show(filters=filters)
            registry_manager.dataset_manager.show(filters=filters)
            registry_manager.dimension_manager.show(filters=filters)
            registry_manager.dimension_mapping_manager.show(filters=filters)

    # test max_width and drop_fields
    registry_manager.show(max_width=40, drop_fields=["Submitter", "Date", "Datasets"])
    registry_manager.show(max_width={"Description": 30, "Date": 20})


def test_registry_show_records(load_registry_manager):
    """dsgrid registry dimensions|dimension-mappings show prints the records table."""
    conn, manager = load_registry_manager
    dim_id = next(
        x.dimension_id
        for x in manager.dimension_manager.db.iter_models()
        if x.dimension_type.value == "geography"
    )
    mapping_id = next(x.mapping_id for x in manager.dimension_mapping_manager.db.iter_models())
    runner = CliRunner()
    for group, config_id, expected in (
        ("dimensions", dim_id, "name"),
        ("dimension-mappings", mapping_id, "from_id"),
    ):
        result = runner.invoke(cli, ["--url", conn.url, "registry", group, "show", config_id])
        assert result.exit_code == 0, result.output
        assert expected in result.output
        assert "only showing top" not in result.output


def test_registry_show_records_truncated(load_registry_manager, monkeypatch):
    """dsgrid registry dimension-mappings show notes when it truncates the records table."""
    conn, manager = load_registry_manager
    mapping_id = next(x.mapping_id for x in manager.dimension_mapping_manager.db.iter_models())
    monkeypatch.setattr(dsgrid.cli.registry, "MAX_RECORDS_TO_SHOW", 1)
    result = CliRunner().invoke(
        cli, ["--url", conn.url, "registry", "dimension-mappings", "show", mapping_id]
    )
    assert result.exit_code == 0, result.output
    assert "only showing top 1 rows" in result.output
