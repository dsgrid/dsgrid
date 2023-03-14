from dsgrid.config.simple_models import RegistrySimpleModel
from dsgrid.dimension.base_models import DimensionType
from dsgrid.registry.registry_database import DatabaseConnection, RegistryDatabase
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.registry.filter_registry_manager import FilterRegistryManager


PROJECT_ID = "test_efs"
DATASET_ID = "test_efs_comstock"
COUNTY_ID = "08031"
STATE_ID = "CO"
QUERY_NAME = "state"
FILTER_CONFIG = {
    "name": "test-registry",
    "projects": [
        {
            "project_id": PROJECT_ID,
            "dimensions": {
                "base_dimensions": [{"dimension_type": "geography", "record_ids": [COUNTY_ID]}],
                "supplemental_dimensions": [
                    {
                        "dimension_query_name": QUERY_NAME,
                        "dimension_type": "geography",
                        "record_ids": [STATE_ID],
                    }
                ],
            },
        }
    ],
    "datasets": [
        {
            "dataset_id": DATASET_ID,
            "dimensions": [{"dimension_type": "geography", "record_ids": [COUNTY_ID]}],
        }
    ],
}


def test_filter_registry(tmp_path):
    simple_model = RegistrySimpleModel(**FILTER_CONFIG)
    dst_data_path = tmp_path / "test-dsgrid-registry"
    src_conn = DatabaseConnection(database="test-dsgrid")
    dst_conn = DatabaseConnection(database="filtered-dsgrid")
    RegistryManager.copy(src_conn, dst_conn, dst_data_path, force=True)
    FilterRegistryManager.load(dst_conn, offline_mode=True).filter(simple_model)
    mgr = RegistryManager.load(dst_conn, offline_mode=True)
    project = mgr.project_manager.load_project(PROJECT_ID)

    try:
        # Verify that the dataset, dimensions, and dimension mappings are all filtered.
        project.load_dataset(DATASET_ID)
        dataset = project.get_dataset(DATASET_ID)
        load_data_df = dataset._handler._load_data
        load_data_lookup_df = dataset._handler._load_data_lookup
        df = load_data_df.join(load_data_lookup_df, on="id").drop("id")
        dataset_geos = df.select("geography").distinct().collect()
        assert len(dataset_geos) == 1
        assert dataset_geos[0].geography == COUNTY_ID

        base_dim = project.config.get_base_dimension(DimensionType.GEOGRAPHY)
        records = base_dim.get_records_dataframe().collect()
        assert len(records) == 1
        assert records[0].id == COUNTY_ID

        supp_dim = project.config.get_dimension_records(QUERY_NAME).collect()
        assert len(supp_dim) == 1
        assert supp_dim[0].id == STATE_ID

        found_mapping_records = False
        for dim in project.config.list_supplemental_dimensions(DimensionType.GEOGRAPHY):
            if dim.model.dimension_query_name == QUERY_NAME:
                for mapping in project.config.get_base_to_supplemental_dimension_mappings_by_types(
                    DimensionType.GEOGRAPHY
                ):
                    if mapping.model.to_dimension.dimension_id == dim.model.dimension_id:
                        records = mapping.get_records_dataframe().collect()
                        assert len(records) == 1
                        assert records[0].from_id == COUNTY_ID and records[0].to_id == STATE_ID
                        found_mapping_records = True
        assert found_mapping_records
    finally:
        RegistryDatabase.delete(dst_conn)
