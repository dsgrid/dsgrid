import os

from dsgrid.utils.files import dump_data, load_data


def test_dump_load_data():
    filenames = ["test_dump_load_data" + ext for ext in (".json", ".json5")]
    try:
        data = {"test": [1, 2, 3]}
        for filename in filenames:
            assert not os.path.exists(filename)
            dump_data(data, filename)
            assert os.path.exists(filename)
            assert load_data(filename) == data
    finally:
        for filename in filenames:
            if os.path.exists(filename):
                os.remove(filename)
