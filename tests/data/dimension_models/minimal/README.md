# Purpose
This is a minimal dataset to test the source code.

## Example usage

```python
"""Example code to explore the default dataset."""

from tests.data.dimension_models.minimal.models import *
from dsgrid.dimension.store import *
from dsgrid.loggers import setup_logging

logger = setup_logging("test", None, console_level=logging.INFO)
store = DimensionStore.load(
    MODEL_MAPPINGS,
    one_to_many=ONE_TO_MANY,
    one_to_one=ONE_TO_ONE,
    programmatic_one_to_one=PROGRAMMATIC_ONE_TO_ONE,
)
```
