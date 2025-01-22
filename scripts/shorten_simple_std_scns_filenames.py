"""
Shorten long filenames in the test data repository in order to ensure the repository
can be cloned on Windows computers where the path limit is 260 characters.
"""

from pathlib import Path


def main():
    path = Path("filtered_registries") / "simple_standard_scenarios" / "data"
    for dataset_path in path.iterdir():
        if dataset_path.is_dir():
            for table_name in ("load_data.parquet", "load_data_lookup.parquet", "table.parquet"):
                data_dir = dataset_path / "1.0.0" / table_name
                if data_dir.exists():
                    parquet_files = list(data_dir.glob("*.parquet"))
                    assert len(parquet_files) == 1, parquet_files
                    filename = parquet_files[0]
                    new_name = filename.with_name("p.parquet")
                    filename.rename(new_name)
                    print(f"Renamed {filename} to {new_name}")
                    crc_files = list(data_dir.glob(".part*.crc"))
                    assert len(crc_files) == 1, crc_files
                    crc_file = crc_files[0]
                    new_name = crc_file.with_name(".p.crc")
                    crc_file.rename(new_name)
                    print(f"Renamed {crc_file} to {new_name}")


if __name__ == "__main__":
    main()
