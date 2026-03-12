# How to Convert Time Zones

dsgrid provides time zone conversion through [Chronify](https://github.com/NREL/chronify), a time series mapping library. The relevant functions live in `dsgrid/utils/dataset.py`. This guide covers when and how to use them, what the input and output time columns look like, and how the query backend affects the output.

## Why the output is always timezone-naive

Both conversion functions described below produce a **timezone-naive** (`timestamp_ntz`) output timestamp column, even though the input is timezone-aware (`timestamp_tz`). This is intentional.

Query backends (DuckDB, Spark) display `timestamp_tz` columns relative to the **system session time zone**, not the input offset or time zone. This means that if you keep timestamps tz-aware after conversion, the value you see when you query the table depends on where the query is run, making the conversion effectively invisible. To make the local time unambiguous and portable, dsgrid converts back to tz-naive timestamps, pairing them with a `time_zone` column that records the target zone for each row.

In summary:

| Column | Type before conversion | Type after conversion |
|---|---|---|
| `timestamp` | `timestamp_tz` (absolute UTC instant) | `timestamp_ntz` (local clock time in target zone) |
| `time_zone` | absent | `STRING` (IANA zone name, e.g. `"Etc/GMT+5"`) |

---

## Two conversion patterns

### 1. `convert_time_zone` — single target time zone

Use this when you want to convert all rows to the **same** target time zone.

- **Input**: a DataFrame with a `timestamp_tz` column.
- **Output**: a DataFrame with a `timestamp_ntz` column (local time in the target zone) and a `time_zone` column containing the target zone name.

**Example** — convert a nationally aggregated hourly dataset to US Mountain Standard Time (`Etc/GMT+7`):

```python
from zoneinfo import ZoneInfo
from dsgrid.utils.dataset import convert_time_zone_with_chronify_duckdb
from dsgrid.utils.scratch_dir_context import ScratchDirContext

mst = ZoneInfo("Etc/GMT+7")
result_df = convert_time_zone_with_chronify_duckdb(
    df=load_data_df,           # DataFrame with timestamp_tz column
    from_time_dim=time_dim,    # DateTimeDimensionConfig describing the input
    time_zone=mst,
    scratch_dir_context=ScratchDirContext(scratch_dir),
    value_column="value",
)
```

After conversion `result_df` has:

```
timestamp            | time_zone  | geography | value
---------------------|------------|-----------|------
2012-01-01 00:00:00  | Etc/GMT+7  | g1        | 1.2
2012-01-01 01:00:00  | Etc/GMT+7  | g1        | 0.9
...
```

---

### 2. `convert_time_zone_by_column` — per-row target time zone

Use this when different rows should be converted to **different** target time zones, driven by a `time_zone` column already in the DataFrame (typically added by `add_time_zone` from a geography dimension).

- **Input**: a DataFrame with a `timestamp_tz` column **and** a `time_zone` column that contains the target IANA zone name for each row.
- **Output**: a DataFrame with a `timestamp_ntz` column (local clock time in each row's target zone); the `time_zone` column is preserved as-is.

**Example** — convert a dataset so each geography's timestamps reflect its local standard time:

```python
from dsgrid.utils.dataset import (
    add_time_zone,
    convert_time_zone_by_column_with_chronify_duckdb,
)
from dsgrid.utils.scratch_dir_context import ScratchDirContext

# geography_dim must have a time_zone record field
df_with_tz = add_time_zone(load_data_df, geography_dim)

result_df = convert_time_zone_by_column_with_chronify_duckdb(
    df=df_with_tz,
    from_time_dim=time_dim,
    scratch_dir_context=ScratchDirContext(scratch_dir),
    value_column="value",
    time_zone_column="time_zone",   # default; column added by add_time_zone
)
```

After conversion `result_df` has:

```
timestamp            | time_zone  | geography | value
---------------------|------------|-----------|------
2012-01-01 00:00:00  | Etc/GMT+5  | g_east    | 1.2
2012-01-01 00:00:00  | Etc/GMT+8  | g_west    | 3.1
...
```

The same absolute UTC instant (`2012-01-01 05:00:00+00`) appears as midnight Eastern or midnight Pacific depending on the row's `time_zone`.

### `wrap_time_allowed`

The `convert_time_zone_by_column` functions accept an optional `wrap_time_allowed: bool` parameter (default `False`). It controls what happens when the per-row timezone offsets shift some timestamps **outside** the nominal time range of the source data.

**Why this arises**: suppose your tz-aware source data covers the calendar year 2012 in UTC. Converting a row in Eastern time (`UTC-5`) shifts the first 5 hours backward to `2011-12-31 19:00–23:00 EST`, which falls before the 2012 boundary. Without wrapping, those hours are preserved at their true local clock positions, so the output starts on `2011-12-31` for Eastern rows.

**Effect of `wrap_time_allowed=True`**: Chronify reorders the timestamps cyclically so the output covers the *same nominal range* as the source schema in tz-naive clock time. The 5 early-morning hours that would have landed in 2011 are instead placed at the end of the year (after `2012-12-31 19:00 EST`). The result is still 8760 hours, all within 2012, just reordered.

| `wrap_time_allowed` | Behavior |
|---|---|
| `False` (default) | Output timestamps reflect true local clock time; range may shift relative to the source |
| `True` | Output timestamps are wrapped so the local-time range matches the source schema's nominal range |

Use `wrap_time_allowed=True` when the source dataset represents a *repeating typical period* (e.g., a representative year) and you want each geography's output to cover the same nominal period in local time rather than an offset window.

```python
result_df = convert_time_zone_by_column_with_chronify_duckdb(
    df=df_with_tz,
    from_time_dim=time_dim,
    scratch_dir_context=ScratchDirContext(scratch_dir),
    value_column="value",
    time_zone_column="time_zone",
    wrap_time_allowed=True,   # keep each geography's output within the same nominal year
)
```

---

## Choosing between the two patterns

| Scenario | Use |
|---|---|
| All data should reflect the same local time | `convert_time_zone` |
| Each geography has its own local time zone | `convert_time_zone_by_column` |

---

## Query backend

Each conversion function has three variants that differ only in how they interact with the query backend. You usually do not call these directly; higher-level dsgrid code selects the appropriate one based on `dsgrid.runtime_config`.

| Backend | Function suffix |
|---|---|
| DuckDB (default, in-memory) | `_with_chronify_duckdb` |
| Spark + Hive Metastore | `_with_chronify_spark_hive` |
| Spark + local filesystem | `_with_chronify_spark_path` |

The DuckDB variants hold everything in memory and are the fastest for moderate-sized datasets. The Spark variants write intermediate Parquet files and suit large distributed workloads.

---

## Relationship to `localize_time_zone`

Time zone *conversion* and time zone *localization* are inverses:

| Operation | Input | Output |
|---|---|---|
| `localize_time_zone` | `timestamp_ntz` | `timestamp_tz` |
| `convert_time_zone` | `timestamp_tz` | `timestamp_ntz` + `time_zone` column |

Localization is applied automatically during dataset registration when the time dimension's `column_format.dtype` is `timestamp_ntz` and the `time_zone_format` identifies one or more target zones. Conversion is applied in query results to express the registered absolute times back in local clock time.
