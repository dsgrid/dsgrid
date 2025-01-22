import json
import sys

from pyarrow import parquet

if len(sys.argv) == 1:
    print("Usage: python {__FILE__} PARQUET_FILE")
    sys.exit(1)

filename = sys.argv[1]
metadata = parquet.read_metadata(filename)
found_time = False
for i in range(metadata.row_group(0).num_columns):
    column = metadata.row_group(0).column(i)
    if column.physical_type == "INT96":
        print(
            f"Column {column.path_in_schema} is of physical type INT96 and is likely a Spark UTC timestamp."
        )
        found_time = True
        continue
    statistics = column.statistics
    if statistics is None:
        continue
    logical_type = metadata.row_group(0).column(i).statistics.logical_type
    if logical_type.type == "TIMESTAMP":
        print(f"Column {column.path_in_schema} is a timestamp column.")
        print(json.dumps(json.loads(logical_type.to_json()), indent=2))
        found_time = True

if not found_time:
    print(f"Did not find a time column in {filename=}")
    sys.exit(1)
