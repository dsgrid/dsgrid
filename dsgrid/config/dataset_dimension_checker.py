import io
import itertools
import logging
from contextlib import redirect_stdout
from typing import List

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from dsgrid.dataset import Dataset
from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGInvalidDataset
from dsgrid.utils.spark import models_to_dataframe, create_dataframe_from_dimension_ids
from dsgrid.utils.timing import track_timing, timer_stats_collector, Timer
from .dataset_config import DatasetConfig
from .dimension_mapping_base import DimensionMappingReferenceModel
from .project_config import ProjectConfig
from .dataset_schema_handler_factory import make_dataset_schema_handler


logger = logging.getLogger(__name__)


@track_timing(timer_stats_collector)
def check_dataset_dimensions_against_project(
    project_config: ProjectConfig,
    dataset_config: DatasetConfig,
    mapping_references: List[DimensionMappingReferenceModel],
    dimension_mgr,
    dimension_mapping_mgr,
):
    """Check that a dataset has all project-required dimension records."""
    handler = make_dataset_schema_handler(dataset_config, dimension_mgr, dimension_mapping_mgr)
    pivot_dimension = handler.get_pivot_dimension_type()
    exclude_dims = set([DimensionType.TIME, DimensionType.DATA_SOURCE, pivot_dimension])
    dimension_pairs = set()
    types = [x for x in DimensionType if x not in exclude_dims]
    for type1, type2 in itertools.product(types, types):
        if type1 != type2:
            dimension_pairs.add(tuple(sorted((type1, type2))))

    data_source = dataset_config.model.data_source
    associations = project_config.dimension_associations
    dim_table = handler.make_dimension_table(mapping_references)
    for type1, type2 in dimension_pairs:
        records = associations.get_associations_by_data_source(data_source, type1, type2)
        if records is None:
            records = _get_project_dimensions_table(project_config, type1, type2)
        columns = (type1.value, type2.value)
        with Timer(timer_stats_collector, "evaluate dimension record counts"):
            record_count = records.count()
            count = records.select(*columns).intersect(dim_table.select(*columns)).count()
        if count != record_count:
            table = records.select(*columns).exceptAll(dim_table.select(*columns))
            dataset_id = dataset_config.model.dataset_id
            with io.StringIO() as buf, redirect_stdout(buf):
                table.show(n=table.count())
                logger.error(
                    "Dataset %s is missing records for %s:\n%s",
                    dataset_id,
                    (type1, type2),
                    buf.getvalue(),
                )
            raise DSGInvalidDataset(
                f"Dataset {dataset_id} is missing records for {(type1, type2)}"
            )
    _check_pivot_dimension_columns(project_config, handler, pivot_dimension, mapping_references)


def _get_project_dimensions_table(project_config, type1, type2):
    pdim1 = project_config.get_base_dimension(type1)
    pdim1_ids = pdim1.get_unique_ids()
    pdim2 = project_config.get_base_dimension(type2)
    pdim2_ids = pdim2.get_unique_ids()
    records = itertools.product(pdim1_ids, pdim2_ids)
    return create_dataframe_from_dimension_ids(records, type1, type2)


def _check_pivot_dimension_columns(project_config, handler, dim_type, mapping_refs):
    d_dim_ids = set(handler.get_pivot_dimension_columns_mapped_to_project(mapping_refs).values())
    p_dim_ids = project_config.get_base_dimension(dim_type).get_unique_ids()
    diff = d_dim_ids.symmetric_difference(p_dim_ids)
    if diff:
        raise DSGInvalidDataset(
            f"load data pivoted columns must be symmetric with project dimensions: {diff}"
        )
