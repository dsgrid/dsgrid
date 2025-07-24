import logging
from pathlib import Path
from typing import Iterable

from chronify.utils.path_utils import check_overwrite

from dsgrid.dimension.time import TimeDimensionType
from dsgrid.exceptions import DSGInvalidParameter
from dsgrid.utils.files import dump_data
from dsgrid.config.project_config import make_unvalidated_project_config


logger = logging.getLogger(__name__)


def generate_project_config(
    project_id: str,
    dataset_ids: Iterable[str],
    metric_types: Iterable[str],
    name: str | None = None,
    description: str | None = None,
    time_type: TimeDimensionType = TimeDimensionType.DATETIME,
    output_directory: Path | None = None,
    overwrite: bool = False,
):
    """Generate project config files and filesystem skeleton."""
    if not metric_types:
        msg = "At least one metric type must be passed"
        raise DSGInvalidParameter(msg)
    output_dir = (output_directory or Path()) / project_id
    check_overwrite(output_dir, overwrite)
    output_dir.mkdir()
    project_dir = output_dir / "project"
    project_dir.mkdir()
    project_file = project_dir / "project.json5"
    datasets_dir = output_dir / "datasets"
    datasets_dir.mkdir()
    (datasets_dir / "historical").mkdir()
    (datasets_dir / "modeled").mkdir()
    dimensions_dir = project_dir / "dimensions"
    dimensions_dir.mkdir()
    (dimensions_dir / "subset").mkdir()
    (dimensions_dir / "supplemental").mkdir()
    (project_dir / "dimension_mappings").mkdir()

    config = make_unvalidated_project_config(
        project_id,
        dataset_ids,
        metric_types,
        name=name,
        description=description,
        time_type=time_type,
    )
    dump_data(config, project_file, indent=2)
    logger.info(
        "Created project directory structure at %s with config file %s", output_dir, project_file
    )
