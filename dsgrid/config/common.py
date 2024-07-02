import json
from typing import Optional

from pydantic import ValidationInfo

from dsgrid.data_models import DSGBaseModel
from dsgrid.utils.files import compute_hash


def compute_hash_from_model_records(file_hash: Optional[str], info: ValidationInfo) -> str:
    """Compute file hash from the model."""
    if file_hash:
        return file_hash

    if not info.data["records"]:
        raise ValueError("records cannot be empty")

    if isinstance(info.data["records"][0], DSGBaseModel):
        records = [x.model_dump(mode="json") for x in info.data["records"]]
    else:
        records = info.data["records"]

    return compute_hash(bytes(json.dumps(records), encoding="utf-8"))
