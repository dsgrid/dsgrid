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


def test_expected_cardinalities_prevents_false_single_column_patterns(tmp_path):
    """Test that expected_cardinalities prevents a geography from being falsely
    reported as a single-column closed pattern when it has data for some
    subsectors but not all.

    Without expected_cardinalities, the closure check uses only the subsector
    values present in the missing data (2 subsectors). A geography missing both
    of those subsectors appears "closed" since it covers the full cross-product
    of missing-data column values. With expected_cardinalities telling the
    algorithm there are actually 4 subsectors, the pattern is no longer closed
    at the single-column level and must be expanded to (geography, subsector).
    """
    # 3 geographies, 4 subsectors, 2 metrics.
    # Full cross-product = 3 × 4 × 2 = 24 rows.
    # Subsectors A and B have full coverage for geo1 and geo2, but are missing
    # for geo3. Subsectors C and D have full coverage for all 3 geographies.
    # So the missing data only contains subsectors A and B.
    # Without expected_cardinalities: dict_sizes[subsector] = 2 (A, B).
    #   geo3 covers 2 subsectors × 2 metrics = 4 rows = full cross-product
    #   of remaining columns → falsely marked as a closed single-column pattern.
    # With expected_cardinalities: effective subsector cardinality = 4.
    #   geo3 would need 4 × 2 = 8 rows to be closed, but only has 4 → not closed.
    #   Correctly reported as (geography, subsector) pairs instead.

    missing_rows = []
    for metric in ["m1", "m2"]:
        # geo3 is missing subsectors A and B
        missing_rows.append({"geography": "geo3", "subsector": "A", "metric": metric})
        missing_rows.append({"geography": "geo3", "subsector": "B", "metric": metric})

    df = pd.DataFrame(missing_rows)
    parquet_path = tmp_path / "missing.parquet"
    df.to_parquet(parquet_path, index=False)

    # Without expected_cardinalities: should produce geography.csv with "geo3"
    output_no_card = tmp_path / "output_no_card"
    find_minimal_patterns_from_file(parquet_path, output_dir=output_no_card)
    geography_file = output_no_card / "geography.csv"
    assert geography_file.exists(), (
        "Without expected_cardinalities, geo3 should be a single-column closed pattern"
    )

    # With expected_cardinalities: geo3 should NOT be a single-column pattern
    output_with_card = tmp_path / "output_with_card"
    expected_cardinalities = {
        "geography": 3,   # geo1, geo2, geo3
        "subsector": 4,   # A, B, C, D (C and D have no missing data)
        "metric": 2,      # m1, m2
    }
    find_minimal_patterns_from_file(
        parquet_path,
        output_dir=output_with_card,
        expected_cardinalities=expected_cardinalities,
    )

    output_files = sorted(f.name for f in output_with_card.iterdir())
    assert "geography.csv" not in output_files, (
        "With expected_cardinalities, geo3 should NOT be a single-column closed pattern. "
        f"Output files: {output_files}"
    )

    # Instead, geo3's missing data should be captured as (geography, subsector) pairs
    geo_sub_file = output_with_card / "geography__subsector.csv"
    assert geo_sub_file.exists(), (
        f"Expected geography__subsector.csv but got: {output_files}"
    )
    geo_sub_df = pd.read_csv(geo_sub_file, dtype=str)
    geo_sub_rows = sorted([tuple(row) for row in geo_sub_df.values])
    assert geo_sub_rows == [("geo3", "A"), ("geo3", "B")]
