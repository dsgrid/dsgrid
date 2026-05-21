import math

from dsgrid.ibis.partition import TablePartition
from dsgrid.ibis.session import create_dataframe_from_dicts


def test_table_partition_sizes():
    table = create_dataframe_from_dicts(
        [
            {"id": "a", "region": "west", "value": 1.0},
            {"id": "b", "region": "west", "value": 2.0},
            {"id": "c", "region": "east", "value": 3.0},
        ]
    )
    partition = TablePartition()

    n_rows, n_cols, data_mb = partition.get_data_size(table, bytes_per_cell=4)
    assert n_rows == 3
    assert n_cols == 3
    assert math.isclose(data_mb, 36 / 1e6)

    assert partition.get_optimal_number_of_files(table, MB_per_cmp_file=1, cmp_ratio=0.5) == 1

    report = partition.file_size_if_partition_by(table, "region")
    assert report["region"]["n_partitions"] == 2
    assert report["region"]["avg_partition_MB"] == 0.0
    assert report["region"]["max_partition_MB"] == 0.0
    assert report["region"]["min_partition_MB"] == 0.0
