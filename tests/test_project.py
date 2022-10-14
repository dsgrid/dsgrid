import pytest
from pyspark.sql import SparkSession
from collections import defaultdict

from dsgrid.project import Project
from dsgrid.dataset.dataset import Dataset
from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGValueNotRegistered, DSGInvalidDimensionMapping
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.tests.common import TEST_REGISTRY


PROJECT_ID = "test_efs"
DATASET_ID = "test_efs_comstock"


def test_project_load():
    mgr = RegistryManager.load(TEST_REGISTRY, offline_mode=True)
    project = mgr.project_manager.load_project(PROJECT_ID)
    assert isinstance(project, Project)

    config = project.config
    dim = config.get_base_dimension(DimensionType.GEOGRAPHY)
    assert dim.model.dimension_type == DimensionType.GEOGRAPHY
    supp_dims = config.list_supplemental_dimensions(DimensionType.GEOGRAPHY)
    assert len(supp_dims) == 4
    assert config.has_base_to_supplemental_dimension_mapping_types(DimensionType.GEOGRAPHY)
    mappings = config.get_base_to_supplemental_dimension_mappings_by_types(DimensionType.GEOGRAPHY)
    assert len(mappings) == 4
    assert config.has_base_to_supplemental_dimension_mapping_types(DimensionType.SECTOR)
    assert config.has_base_to_supplemental_dimension_mapping_types(DimensionType.SUBSECTOR)

    records = project.config.get_dimension_records("all_subsectors").collect()
    assert len(records) == 1
    assert records[0].id == "all_subsectors"

    table = project.config.load_dimension_associations(DATASET_ID)
    assert table.count() > 0

    with pytest.raises(DSGValueNotRegistered):
        project = mgr.project_manager.load_project(PROJECT_ID, version="0.0.0")
        assert isinstance(project, Project)


def test_dataset_load():
    mgr = RegistryManager.load(TEST_REGISTRY, offline_mode=True)
    project = mgr.project_manager.load_project(PROJECT_ID)
    project.load_dataset(DATASET_ID)
    dataset = project.get_dataset(DATASET_ID)

    assert isinstance(dataset, Dataset)
    spark = SparkSession.getActiveSession()
    data = spark.sql(f"select * from {DATASET_ID}__load_data")
    assert "timestamp" in data.columns
    assert "com_fans" in data.columns
    lookup = spark.sql(f"select * from {DATASET_ID}__load_data_lookup")
    assert "subsector" in lookup.columns
    assert "id" in lookup.columns

    query_names = sorted(
        project.config.list_dimension_query_names_by_type(DimensionType.GEOGRAPHY)
    )
    assert query_names == [
        "all_geographies",
        "census_division",
        "census_region",
        "county",
        "state",
    ]
    records = project.config.get_dimension_records("state")
    assert records.filter("id = 'CO'").count() > 0

    table_name = DATASET_ID + "__" + "load_data"
    assert spark.catalog.tableExists(table_name)
    project.unload_dataset(DATASET_ID)
    assert not spark.catalog.tableExists(table_name)


def test_dimension_map_and_reduce_in_dataset():
    mgr = RegistryManager.load(TEST_REGISTRY, offline_mode=True)
    project = mgr.project_manager.load_project(PROJECT_ID)
    project.load_dataset(DATASET_ID)
    dataset = project.get_dataset(DATASET_ID)

    mapped_load_data = dataset._handler._remap_dimension_columns(dataset.load_data)
    mapped_load_data_lookup = dataset._handler._remap_dimension_columns(dataset.load_data_lookup)

    # [1] check that mapped tables contain all to_id records from mappings
    table_is_lookup = False
    for ref in dataset._handler._mapping_references:
        column = ref.from_dimension_type.value
        mapping_config = dataset._handler._dimension_mapping_mgr.get_by_id(ref.mapping_id)
        to_records = mapping_config.get_unique_to_ids()  # set

        if column == dataset._handler.get_pivoted_dimension_type().value:
            diff = to_records.difference(mapped_load_data.columns)
        else:
            if column in mapped_load_data_lookup.columns:
                diff = set(
                    [
                        row[column]
                        for row in mapped_load_data_lookup.select(column).distinct().collect()
                    ]
                ).symmetric_difference(to_records)
                table_is_lookup = True
            else:
                diff = set(
                    [row[column] for row in mapped_load_data.select(column).distinct().collect()]
                ).symmetric_difference(to_records)
        if diff:
            table_type = "load_data_lookup" if table_is_lookup else "load_data"
            raise DSGInvalidDimensionMapping(
                "Mapped %s is incorrect, check %s mapping: %s or mapping logic in 'dataset_schema_handler_base._map_and_reduce_dimension()' \n%s"
                % (table_type, column, ref.mapping_id, diff)
            )

    # [2] check that fraction is correctly applied and reduced
    # [2A] load_data_lookup
    assert "fraction" in mapped_load_data_lookup.columns

    # * this check is specific to the actual from_fraction values specified in the mapping *
    data_filters = "data_source=='comstock' and subsector=='Warehouse' and model_year=='2050'"
    fraction = [
        row.fraction
        for row in mapped_load_data_lookup.filter(data_filters)
        .select("fraction")
        .distinct()
        .collect()
    ]
    assert len(fraction) == 1
    assert fraction[0] == (0.9 * 1.3)

    # [2B] load_data
    for ref in dataset._handler._mapping_references:
        column = ref.from_dimension_type.value

        if column == dataset._handler.get_pivoted_dimension_type().value:
            assert "fraction" not in mapped_load_data.columns

            mapping_config = dataset._handler._dimension_mapping_mgr.get_by_id(ref.mapping_id)
            records = mapping_config.model.records

            # apply mapping to load_data.sum(), then compare to mapped_load_data.sum()
            # 2B.1 get total enduse loads from each table
            sum_query = [
                f"SUM({col}) AS {col}" for col in dataset._handler.get_pivoted_dimension_columns()
            ]
            load_data_sum = dataset.load_data.selectExpr(*sum_query)

            sum_query = [f"SUM({col}) AS {col}" for col in mapping_config.get_unique_to_ids()]
            mapped_load_data_sum = mapped_load_data.selectExpr(*sum_query).toPandas()

            # 2B.2 apply mapping
            # this part of the code is the same as 'dataset_schema_handler_base._map_and_reduce_dimension() for pivoted dim mapping'
            records_dict = defaultdict(dict)
            for row in records:
                if row.to_id is not None:
                    records_dict[row.to_id][row.from_id] = row.from_fraction

            to_ids = sorted(records_dict)
            value_operations = []
            for tid in to_ids:
                operation = "+".join(
                    [f"{from_id}*{fraction}" for from_id, fraction in records_dict[tid].items()]
                )  # assumes reduce by summation
                operation += f" AS {tid}"
                value_operations.append(operation)

            load_data_sum = load_data_sum.selectExpr(*value_operations).toPandas()

            # 2B.3 check that the newly mapped load_data_sum = mapped_load_data_sum within tolerance
            decimal_tolerance = 3
            load_data_diff = (
                (load_data_sum - mapped_load_data_sum).round(decimal_tolerance).iloc[0]
            )  # pd.series
            assert len(load_data_diff[load_data_diff != 0]) == 0

        else:
            pass


# def test_aggregate_load_by_state():
#    store = DimensionStore.load(PROJECT_CONFIG_FILE)
#    dataset = Dataset.load(store)
#    df = dataset.aggregate_sector_sums_by_dimension(County, State)
#    assert "state" in df.columns
#    assert "sum((sum(fans) * scale_factor))" in df.columns
#    # For now just ensure this doesn't fail.
#    df.count()
