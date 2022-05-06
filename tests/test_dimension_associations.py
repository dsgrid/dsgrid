import shutil
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from pyspark.sql import DataFrame

from dsgrid.config.dimension_associations import DimensionAssociations
from dsgrid.dimension.base_models import DimensionType


DATA_DIR = Path("tests") / "data" / "dimension_associations"


def test_dimension_associations(spark_session):
    da = DimensionAssociations.load(Path("."), DATA_DIR.iterdir())
    assert isinstance(da.table, DataFrame)
    assert da.has_associations(DimensionType.SECTOR, DimensionType.SUBSECTOR)
    assert da.has_associations(
        DimensionType.SECTOR, DimensionType.SUBSECTOR, data_source="comstock"
    )
    assert not da.has_associations(DimensionType.SECTOR, DimensionType.TIME)
    assert da.list_data_sources() == ["comstock", "resstock", "tempo"]
    assert (
        da.get_associations(
            DimensionType.MODEL_YEAR,
            DimensionType.SECTOR,
            DimensionType.SUBSECTOR,
            data_source="comstock",
        ).count()
        == 294  # 14 sector__subsector * 21 years
    )
    assert (
        da.get_associations(
            DimensionType.MODEL_YEAR,
            DimensionType.SECTOR,
            DimensionType.SUBSECTOR,
            data_source="resstock",
        ).count()
        == 105  # 5 sector__subsector * 21 years
    )
    assert (
        da.get_associations(
            DimensionType.MODEL_YEAR,
            DimensionType.SECTOR,
            DimensionType.SUBSECTOR,
            data_source="tempo",
        ).count()
        == 136  # 8 sector__subsector * 17 years
    )
    assert (
        da.get_associations(
            DimensionType.MODEL_YEAR, DimensionType.SECTOR, DimensionType.SUBSECTOR
        ).count()
        == 294 + 105 + 136
    )


# TODO: This is no longer working with the latest code. Need to redesign the test.
@pytest.mark.skip
def test_dimension_associations_three_dims(spark_session):
    with TemporaryDirectory() as tmpdir:
        dst = Path(tmpdir) / "dimension_associations"
        shutil.copytree(DATA_DIR, dst)
        old_file = dst / "sector__subsector.csv"
        new_file = dst / "sector__subsector__model_year.csv"
        lines = old_file.read_text().splitlines()
        lines[0] += ",model_year"
        for i, _ in enumerate(lines[1:]):
            lines[i + 1] += ",2018"  # This model year is in all data sources.
        old_file.unlink()
        for path in list(dst.iterdir()):
            if "model_year" in path.name and "data_source" not in path.name:
                path.unlink()
        new_file.write_text("\n".join(lines))
        da = DimensionAssociations.load(dst, [x.name for x in dst.iterdir()])
        assert (
            da.get_associations(
                DimensionType.MODEL_YEAR, DimensionType.SECTOR, DimensionType.SUBSECTOR
            ).count()
            == 27
        )
