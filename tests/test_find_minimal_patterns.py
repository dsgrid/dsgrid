import pandas as pd

from dsgrid.rust_ext import find_minimal_patterns_from_file

EXPECTED_GEOGRAPHY_SUBSECTOR = [
    ("06075", "com__QuickServiceRestaurant"),
    ("08031", "com__QuickServiceRestaurant"),
    ("08031", "com__SmallHotel"),
    ("36001", "com__QuickServiceRestaurant"),
    ("36119", "com__SmallHotel"),
    ("36119", "com__Hospital"),
]

EXPECTED_SECTOR_SUBSECTOR = [
    ("res", "com__LargeHotel"),
    ("res", "com__StripMall"),
    ("res", "com__QuickServiceRestaurant"),
    ("res", "com__MediumOffice"),
    ("res", "com__StandaloneRetail"),
    ("res", "com__FullServiceRestaurant"),
    ("res", "com__LargeOffice"),
    ("res", "com__SmallHotel"),
    ("res", "com__Warehouse"),
    ("res", "com__Hospital"),
    ("res", "com__SmallOffice"),
    ("res", "com__PrimarySchool"),
    ("res", "com__Outpatient"),
    ("com", "com__MidriseApartment"),
]


def test_find_minimal_patterns(tmp_path):
    input_csv = "dsgrid-test-data/datasets/test_efs_comstock/full_missing_associations.csv"
    df = pd.read_csv(input_csv, dtype={"geography": str})

    parquet_path = tmp_path / "missing_associations.parquet"
    df.to_parquet(parquet_path, index=False)

    output_dir = tmp_path / "missing_associations"
    find_minimal_patterns_from_file(parquet_path, output_dir=output_dir)

    assert len(list(output_dir.iterdir())) == 2
    geography_subsector_file = output_dir / "geography__subsector.csv"
    sector_subsector_file = output_dir / "sector__subsector.csv"

    assert geography_subsector_file.exists(), "geography__subsector.csv should exist"
    assert sector_subsector_file.exists(), "sector__subsector.csv should exist"

    geography_df = pd.read_csv(geography_subsector_file, dtype=str)
    geography_rows = sorted([tuple(row) for row in geography_df.values])
    expected_geography_rows = sorted(EXPECTED_GEOGRAPHY_SUBSECTOR)
    assert geography_rows == expected_geography_rows, (
        f"geography__subsector.csv content mismatch:\n"
        f"Expected: {expected_geography_rows}\n"
        f"Got: {geography_rows}"
    )

    sector_df = pd.read_csv(sector_subsector_file, dtype=str)
    sector_rows = sorted([tuple(row) for row in sector_df.values])
    expected_sector_rows = sorted(EXPECTED_SECTOR_SUBSECTOR)
    assert sector_rows == expected_sector_rows, (
        f"sector__subsector.csv content mismatch:\n"
        f"Expected: {expected_sector_rows}\n"
        f"Got: {sector_rows}"
    )
