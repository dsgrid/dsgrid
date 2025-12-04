import pytest

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
