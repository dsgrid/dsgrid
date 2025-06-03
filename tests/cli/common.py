from pathlib import Path
from typing import Type

from dsgrid.data_models import DSGBaseModel
from dsgrid.utils.files import load_data


def check_config_fields(config_file: Path, model_cls: Type[DSGBaseModel]) -> None:
    config = load_data(config_file)
    actual_fields = set(config.keys())
    required_fields: set[str] = set()
    all_fields: set[str] = set()
    for name, field_info in model_cls.model_fields.items():
        all_fields.add(name)
        if field_info.is_required:
            required_fields.add(name)
    assert not actual_fields - all_fields
    assert required_fields - actual_fields
