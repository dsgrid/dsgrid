import shutil
from pathlib import Path
from tempfile import TemporaryDirectory

from dsgrid.config.dimension_associations import DimensionAssociations
from dsgrid.dimension.base_models import DimensionType


DATA_DIR = Path("tests") / "data" / "dimension_associations"


def test_dimension_associations(spark_session):
    da = DimensionAssociations.load(Path("."), DATA_DIR.iterdir())
    assert da.has_associations(DimensionType.SECTOR, DimensionType.SUBSECTOR)
    assert not da.has_associations(DimensionType.SECTOR, DimensionType.TIME)
    assert da.get_associations(DimensionType.SECTOR, DimensionType.SUBSECTOR).count() == 14
    assert (
        da.get_associations_by_data_source(
            "comstock", DimensionType.SECTOR, DimensionType.SUBSECTOR
        ).count()
        == 14
    )


def test_dimension_associations_three_dims(spark_session):
    with TemporaryDirectory() as tmpdir:
        dst = Path(tmpdir) / "dimension_associations"
        shutil.copytree(DATA_DIR, dst)
        old_file = dst / "sector__subsector.csv"
        new_file = dst / "sector__subsector__model_year.csv"
        lines = old_file.read_text().splitlines()
        lines[0] += ",model_year"
        for i, _ in enumerate(lines[1:]):
            lines[i + 1] += ",2012"
        new_file.write_text("\n".join(lines))
        old_file.unlink()
        da = DimensionAssociations.load(dst, [x.name for x in dst.iterdir()])
        assert (
            da.get_associations(
                DimensionType.MODEL_YEAR, DimensionType.SECTOR, DimensionType.SUBSECTOR
            ).count()
            == 14
        )
        assert (
            da.get_associations_by_data_source(
                "comstock", DimensionType.MODEL_YEAR, DimensionType.SECTOR, DimensionType.SUBSECTOR
            ).count()
            == 14
        )

        # Check asking for a subset of dimensions.
        assert da.get_associations(DimensionType.SECTOR, DimensionType.SUBSECTOR).count() == 14
        assert (
            da.get_associations_by_data_source(
                "comstock", DimensionType.SECTOR, DimensionType.SUBSECTOR
            ).count()
            == 14
        )
