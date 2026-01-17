#!/usr/bin/env python3
"""Migrate dataset configuration files from old schema to new data_layout schema.

Old structure:
    table_schema:
        data_schema:
            data_schema_type: "standard" | "one_table"
            table_format:
                format_type: "pivoted" | "unpivoted"
                pivoted_dimension_type: "metric"  (only if pivoted)
        data_file: {...}
        lookup_data_file: {...}  (only for standard)
        missing_associations: [...]

New structure:
    data_layout:
        table_format: "two_table" | "one_table"
        value_format: "pivoted" | "stacked"
        pivoted_dimension_type: "metric"  (only if pivoted)
        data_file: {...}
        lookup_data_file: {...}  (only for two_table)
        missing_associations: [...]
"""

import json5
from pathlib import Path


def migrate_dataset_config(data: dict) -> dict:
    """Migrate a dataset config from old schema to new schema."""
    if "table_schema" not in data:
        return data

    table_schema = data.pop("table_schema")
    data_schema = table_schema.get("data_schema", {})

    # Map old schema type to new table format
    old_schema_type = data_schema.get("data_schema_type", "one_table")
    if old_schema_type == "standard":
        new_table_format = "two_table"
    else:
        new_table_format = "one_table"

    # Map old format_type to new value_format
    old_table_format = data_schema.get("table_format", {})
    old_format_type = old_table_format.get("format_type", "unpivoted")
    if old_format_type == "unpivoted":
        new_value_format = "stacked"
    else:
        new_value_format = old_format_type  # "pivoted" stays the same

    # Build new data_layout
    data_layout = {
        "table_format": new_table_format,
        "value_format": new_value_format,
    }

    # Add pivoted_dimension_type if pivoted
    if new_value_format == "pivoted" and "pivoted_dimension_type" in old_table_format:
        data_layout["pivoted_dimension_type"] = old_table_format["pivoted_dimension_type"]

    # Copy data_file
    if "data_file" in table_schema:
        data_layout["data_file"] = table_schema["data_file"]

    # Copy lookup_data_file (only for two_table)
    if "lookup_data_file" in table_schema and table_schema["lookup_data_file"] is not None:
        data_layout["lookup_data_file"] = table_schema["lookup_data_file"]

    # Copy missing_associations
    if "missing_associations" in table_schema and table_schema["missing_associations"]:
        data_layout["missing_associations"] = table_schema["missing_associations"]

    data["data_layout"] = data_layout
    return data


def migrate_file(filepath: Path, dry_run: bool = False) -> bool:
    """Migrate a single file. Returns True if changes were made."""
    with open(filepath, encoding="utf-8") as f:
        content = f.read()

    data = json5.loads(content)

    if "table_schema" not in data:
        print(f"Skipping {filepath}: no table_schema found")
        return False

    migrated_data = migrate_dataset_config(data)

    if dry_run:
        print(f"Would migrate {filepath}")
        print(json5.dumps(migrated_data, indent=2))
        return True

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(json5.dumps(migrated_data, indent=2))
        f.write("\n")

    print(f"Migrated {filepath}")
    return True


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Migrate dataset config files to new schema")
    parser.add_argument(
        "paths",
        nargs="+",
        type=Path,
        help="Paths to dataset.json5 files or directories to search",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be changed without making changes",
    )
    args = parser.parse_args()

    files_to_migrate = []
    for path in args.paths:
        if path.is_file():
            files_to_migrate.append(path)
        elif path.is_dir():
            files_to_migrate.extend(path.rglob("dataset.json5"))

    migrated_count = 0
    for filepath in files_to_migrate:
        if migrate_file(filepath, dry_run=args.dry_run):
            migrated_count += 1

    print(f"\n{'Would migrate' if args.dry_run else 'Migrated'} {migrated_count} files")


if __name__ == "__main__":
    main()
