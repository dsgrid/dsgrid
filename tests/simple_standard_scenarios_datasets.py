"""Contains functions to read and write simple-standard-scenarios datasets."""

from collections import defaultdict, namedtuple
from pathlib import Path

from dsgrid.config.mapping_tables import MappingTableRecordModel
from dsgrid.dimension.base_models import DimensionType
from dsgrid.dataset.dataset_mapping_manager import DatasetMappingManager
from dsgrid.dsgrid_rc import DsgridRuntimeConfig
from dsgrid.registry.common import DatabaseConnection
from dsgrid.registry.registry_database import RegistryDatabase
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.spark.functions import (
    cross_join,
    read_csv,
)
from dsgrid.spark.types import (
    F,
    IntegerType,
)
from dsgrid.utils.files import (
    dump_data,
    load_data,
    load_line_delimited_json,
    dump_line_delimited_json,
)
from dsgrid.utils.scratch_dir_context import ScratchDirContext
from dsgrid.utils.spark import (
    models_to_dataframe,
    get_spark_session,
)
from dsgrid.utils.utilities import convert_record_dicts_to_classes
from dsgrid.tests.utils import read_parquet_two_table_format


REGISTRY_PATH = Path("dsgrid-test-data/filtered_registries/simple_standard_scenarios")
EXPECTED_DATASET_PATH = REGISTRY_PATH / "expected_datasets"
BUILDING_PIVOTED_COLUMNS = ("electricity_cooling", "electricity_heating", "natural_gas_heating")
BUILDING_COUNTY_MAPPING = {
    "06037": "G0600370",
    "06073": "G0600730",
    "36047": "G3600470",
    "36081": "G3600810",
}
STATS_FILENAME = "raw_stats.json"
Datasets = namedtuple("Datasets", ["comstock", "resstock", "tempo"])


def build_expected_datasets():
    """Build the expected datasets and save summarized stats to a JSON file."""
    path = REGISTRY_PATH
    aeo_com = map_aeo_com_subsectors(
        map_aeo_com_county_to_comstock_county(
            duplicate_aeo_com_census_division_to_county(
                apply_load_mapping_aeo_com(
                    read_csv(
                        path
                        / "data"
                        / "aeo2021_reference_commercial_energy_use_growth_factors"
                        / "1.0.0"
                        / "load_data.csv"
                    )
                )
            )
        )
    )
    aeo_res = apply_load_mapping_aeo_res(
        read_csv(
            path
            / "data"
            / "aeo2021_reference_residential_energy_use_growth_factors"
            / "1.0.0"
            / "load_data.csv"
        ).drop("sector")
    )
    comstock = make_projection_df(
        aeo_com,
        read_parquet_two_table_format(path / "data" / "comstock_conus_2022_reference" / "1.0.0"),
        ["geography", "subsector", "model_year"],
    )
    resstock = make_projection_df(
        aeo_res,
        read_parquet_two_table_format(path / "data" / "resstock_conus_2022_reference" / "1.0.0"),
        ["model_year"],
    )
    tempo = build_tempo()
    # Convert to project units - MWh
    for column in ("electricity_cooling", "electricity_heating", "natural_gas_heating"):
        comstock = comstock.withColumn(column, F.col(column) / 1000)
        resstock = resstock.withColumn(column, F.col(column) / 1000)
    tempo = tempo.withColumn("L1andL2", F.col("L1andL2") / 1000)

    datasets = Datasets(
        comstock=comstock,
        resstock=resstock,
        tempo=tempo,
    )
    stats = generate_raw_stats(datasets)
    EXPECTED_DATASET_PATH.mkdir(exist_ok=True)
    filename = EXPECTED_DATASET_PATH / STATS_FILENAME
    dump_data(stats, filename, indent=2)
    print(f"Wrote stats to {filename}")


def load_dataset_stats() -> dict:
    """Load the saved dataset stats."""
    return load_data(EXPECTED_DATASET_PATH / STATS_FILENAME)


def apply_load_mapping_aeo_com(aeo_com):
    return (
        aeo_com.withColumn("electricity_cooling", F.col("elec_cooling") * 1.0)
        .withColumn("electricity_heating", F.col("elec_heating") * 1.0)
        .withColumn("natural_gas_heating", F.col("ng_heating") * 1.0)
        .drop("elec_cooling", "elec_heating", "ng_heating")
    )


def duplicate_aeo_com_census_division_to_county(aeo_com):
    records = get_dim_mapping_records_from_db("US Census Divisions", "US Counties 2020 L48")
    assert records.select("from_fraction").distinct().collect()[0].from_fraction == 1.0
    records = records.drop("from_fraction")
    mapped = aeo_com.join(records, on=aeo_com.geography == records.from_id)
    # Make sure no census division got dropped in the join.
    orig_count = aeo_com.select("geography").distinct().count()
    new_count = mapped.select("geography").distinct().count()
    assert orig_count == new_count, f"{orig_count} {new_count}"
    return mapped.drop("from_id", "geography").withColumnRenamed("to_id", "geography")


def map_aeo_com_county_to_comstock_county(aeo_com):
    records = get_dim_mapping_records_from_db(
        "conus_2022-comstock_US_county_FIP", "US Counties 2020 L48"
    )
    assert records.select("from_fraction").distinct().collect()[0].from_fraction == 1.0
    records = records.drop("from_fraction")
    mapped = aeo_com.join(records, on=aeo_com.geography == records.to_id)
    # Make sure no entries were dropped.
    orig_count = aeo_com.count()
    new_count = mapped.count()
    assert orig_count == new_count, f"{orig_count} {new_count}"
    return mapped.drop("to_id", "geography").withColumnRenamed("from_id", "geography")


def map_aeo_com_subsectors(aeo_com):
    records = get_dim_mapping_records_from_db(
        "AEO2021-commercial-building-types", "CONUS-2022-Detailed-Subsectors"
    )
    mapped = aeo_com.join(records, on=aeo_com.subsector == records.from_id)
    # Make sure no subsector got dropped in the join.
    orig_count = aeo_com.select("subsector").distinct().count()
    new_count = mapped.select("subsector").distinct().count()
    assert orig_count == new_count, f"{orig_count} {new_count}"
    mapped = mapped.drop("from_id", "subsector").withColumnRenamed("to_id", "subsector")
    for col in ("electricity_cooling", "electricity_heating"):
        mapped = mapped.withColumn(col, mapped[col] * mapped["from_fraction"])
    return (
        mapped.drop("from_fraction")
        .groupBy("subsector", "geography")
        .agg(
            F.sum("electricity_cooling").alias("electricity_cooling"),
            F.sum("electricity_heating").alias("electricity_heating"),
            F.sum("natural_gas_heating").alias("natural_gas_heating"),
        )
    )


def get_dim_mapping_records_from_db(from_dim_name, to_dim_name):
    conn = DatabaseConnection(database="simple-standard-scenarios")
    client = RegistryDatabase.connect(conn)
    records = None
    for doc in client.collection("dimension_mappings"):
        from_id = doc["from_dimension"]["dimension_id"]
        to_id = doc["to_dimension"]["dimension_id"]
        from_dim = client.collection("dimensions").find({"dimension_id": from_id}).next()
        to_dim = client.collection("dimensions").find({"dimension_id": to_id}).next()
        if from_dim["name"] == from_dim_name and to_dim["name"] == to_dim_name:
            models = convert_record_dicts_to_classes(doc["records"], MappingTableRecordModel)
            records = models_to_dataframe(models)
    assert records is not None, f"{from_dim_name=} {to_dim_name=}"
    return records


def apply_load_mapping_aeo_res(aeo_res):
    return (
        aeo_res.withColumn("electricity_cooling", F.col("elec_heat_cool") * 1.0)
        .withColumn("electricity_heating", F.col("elec_heat_cool") * 1.0)
        .withColumn("natural_gas_heating", F.col("ng_heat_cool") * 1.0)
        .drop("elec_heat_cool", "ng_heat_cool")
    )


def make_projection_df(aeo, ld_df, join_columns):
    # comstock and resstock have a single year of data for model_year 2018
    # Apply the growth rate for 2020 and 2040, the years in the filtered registry.
    spark = get_spark_session()
    years_df = spark.createDataFrame([{"model_year": "2020"}, {"model_year": "2040"}])
    aeo = cross_join(aeo, years_df)
    ld_df = cross_join(ld_df, years_df)
    base_year = 2018
    gr_df = aeo
    pivoted_columns = BUILDING_PIVOTED_COLUMNS
    for column in pivoted_columns:
        gr_col = column + "__gr"
        gr_df = gr_df.withColumn(
            gr_col,
            F.pow((1 + F.col(column)), F.col("model_year").cast(IntegerType()) - base_year),
        ).drop(column)

    df = ld_df.join(gr_df, on=join_columns)
    for column in pivoted_columns:
        gr_col = column + "__gr"
        df = df.withColumn(column, df[column] * df[gr_col]).drop(gr_col)

    return df


def build_tempo():
    conn = DatabaseConnection(database="simple-standard-scenarios")
    with RegistryManager.load(
        conn,
        offline_mode=True,
    ) as registry_mgr:
        project = registry_mgr.project_manager.load_project("dsgrid_conus_2022")
        dataset_id = "tempo_conus_2022"
        project.load_dataset(dataset_id)
        tempo = project.get_dataset(dataset_id)
        lookup = tempo._handler._load_data_lookup
        load_data = tempo._handler._load_data
        value_columns = tempo._handler.config.get_value_columns()
        assert len(value_columns) == 1
        value_column = next(iter(value_columns))
        input_dataset = project.config.get_dataset(tempo._config.model.dataset_id)
        plan = tempo._handler.build_default_dataset_mapping_plan()
        context = ScratchDirContext(DsgridRuntimeConfig.load().get_scratch_dir())
        with DatasetMappingManager(tempo._handler.dataset_id, plan, context) as mapping_mgr:
            tempo_data_mapped_time = tempo._handler._convert_time_dimension(
                load_data_df=load_data.join(lookup, on="id").drop("id"),
                to_time_dim=project.config.get_base_time_dimension(),
                mapping_manager=mapping_mgr,
                value_column=value_column,
                wrap_time_allowed=input_dataset.wrap_time_allowed,
                time_based_data_adjustment=input_dataset.time_based_data_adjustment,
                to_geo_dim=project.config.get_base_dimension(DimensionType.GEOGRAPHY),
            )
            return tempo_data_mapped_time


def generate_raw_stats(datasets):
    stats = {"overall": defaultdict(dict), "by_county": {}}
    for project_county in BUILDING_COUNTY_MAPPING:
        stats["by_county"][project_county] = defaultdict(dict)

    operations = (F.sum, F.max, F.mean)
    for name in Datasets._fields:
        for op in operations:
            table = getattr(datasets, name)
            perform_op_by_electricity(stats["overall"], table, name, op)
            for project_county in stats["by_county"]:
                if name == "tempo":
                    dataset_county = project_county
                else:
                    dataset_county = BUILDING_COUNTY_MAPPING[project_county]
                _table = table.filter(f"geography='{dataset_county}'")
                perform_op_by_electricity(stats["by_county"][project_county], _table, name, op)

    accumulate_stats(stats["overall"])
    for county_stats in stats["by_county"].values():
        accumulate_stats(county_stats)
    return stats


def accumulate_stats(stats):
    com = stats["comstock"]
    res = stats["resstock"]
    tem = stats["tempo"]
    com["sum"]["electricity"] = (
        com["sum"]["electricity_cooling"] + com["sum"]["electricity_heating"]
    )
    res["sum"]["electricity"] = (
        res["sum"]["electricity_cooling"] + res["sum"]["electricity_heating"]
    )
    tem["sum"]["electricity"] = tem["sum"]["L1andL2"]
    com["max"]["electricity"] = max(
        (com["max"]["electricity_cooling"], com["max"]["electricity_heating"])
    )
    res["max"]["electricity"] = max(
        (res["max"]["electricity_cooling"], res["max"]["electricity_heating"])
    )
    tem["max"]["electricity"] = tem["max"]["L1andL2"]
    stats["comstock_resstock"] = {
        "sum": {
            "electricity_cooling": com["sum"]["electricity_cooling"]
            + res["sum"]["electricity_cooling"],
            "electricity_heating": com["sum"]["electricity_heating"]
            + res["sum"]["electricity_heating"],
            "electricity": com["sum"]["electricity"] + res["sum"]["electricity"],
        },
        "max": {
            "electricity_cooling": max(
                (com["max"]["electricity_cooling"], res["max"]["electricity_cooling"])
            ),
            "electricity_heating": max(
                (com["max"]["electricity_heating"], res["max"]["electricity_heating"])
            ),
            "electricity": max((com["max"]["electricity"], res["max"]["electricity"])),
        },
    }
    stats["total"] = {
        "sum": {
            "electricity": com["sum"]["electricity"]
            + res["sum"]["electricity"]
            + tem["sum"]["electricity"],
        },
        "max": {
            "electricity": max(
                (com["max"]["electricity"], res["max"]["electricity"], tem["max"]["electricity"])
            ),
        },
    }


def perform_op_by_electricity(stats, table, name, operation):
    if name in ("comstock", "resstock"):
        columns = ["electricity_cooling", "electricity_heating"]
    elif name == "tempo":
        columns = ["L1andL2"]
    else:
        assert False, name
    for col in columns:
        op = operation.__name__
        col_name = f"{op}_{col}"
        if op not in stats[name]:
            stats[name][op] = {}
        val = getattr(
            table.agg(operation(col).alias(col_name)).collect()[0],
            col_name,
        )
        if op == "sum":
            # 2 scenarios
            val *= 2
        stats[name][op][col] = val
    stats[name]["count"] = table.count()


def change_dataset_schemas(dataset_schemas: dict):
    filenames = list((REGISTRY_PATH / "dump").glob("datasets_*data.json"))
    assert len(filenames) == 1
    count = 0
    datasets = load_line_delimited_json(filenames[0])
    for dataset in datasets:
        if dataset["dataset_id"] in dataset_schemas:
            dataset["data_schema"] = dataset_schemas[dataset["dataset_id"]]
            count += 1
    assert count == len(dataset_schemas)
    dump_line_delimited_json(datasets, filenames[0])


if __name__ == "__main__":
    build_expected_datasets()
