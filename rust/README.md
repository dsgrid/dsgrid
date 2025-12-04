# dsgrid Rust Extension - Pattern Analysis

This directory contains the Rust extension for high-performance pattern analysis in dsgrid.

## What It Does

The Rust extension provides fast analysis of dimension record patterns using the "minimal closed patterns" algorithm. The "minimal closed patterns" algorithm identifies combinations of dimension records for which full cross joins of the dimension types not represented in the combination exist in the full dataset. The patterns are "minimal" in that each row of the original dataset matches exactly one of the output combinations and that list of output combinations is as short as possible.

Applying this algorithm to a dataset's set of unexpected missing records helps identify root causes of the missing data.

## Building

### Prerequisites

1. **Rust toolchain** (1.70 or later):
   ```bash
   curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
   ```

2. **Maturin**:
   ```bash
   pip install maturin
   ```

### Development Build

From the **dsgrid root directory** (not this rust/ directory):

```bash
# Install in development mode (recommended for development)
maturin develop --release

# Or use pip in editable mode
pip install -e .
```

### Production Build

```bash
# Build wheel
maturin build --release

# Install the wheel
pip install target/wheels/dsgrid_toolkit-*.whl
```

## Testing

### Test the Rust Extension Directly

```python
from dsgrid.rust_ext import find_patterns_from_parquet

# Assuming you have a parquet file with missing records
patterns = find_patterns_from_parquet(
    "test_missing_records.parquet",
    max_depth=3,
    verbose=True
)

for p in patterns:
    print(f"Pattern {p.pattern_id}: {p.columns} = {p.values} ({p.num_rows} rows)")
```

### Test the Integration

The pattern finder is automatically called when `handle_dimension_association_errors`
is invoked in `dsgrid/utils/dataset.py`. It will analyze missing dimension records
and log the top patterns found.

## Performance

The Rust implementation is significantly faster than pure Python:

- **Memory**: ~3-4 GB for 28M rows with 7 columns (vs ~12 GB pure Python)
- **Speed**: 10-100x faster depending on dataset characteristics
- **Parallelism**: Uses all CPU cores via Rayon

## Architecture

### Rust Code (`src/lib.rs`)

- **PyO3 bindings**: Exposes Rust functions to Python
- **Pattern types**: `Pattern` (Python-exposed) and `InternalPattern` (Rust-only)
- **Core algorithm**:
  - Dictionary encoding to u16
  - RoaringBitmap for efficient set operations
  - Parallel pattern expansion with Rayon
  - Closed pattern checking

### Python Wrapper (`dsgrid/rust_ext/__init__.py`)

- User-friendly Python API
- Type hints and documentation
- Error handling

### Integration (`dsgrid/utils/dataset.py`)

- Called from `handle_dimension_association_errors`
- Analyzes missing records parquet file
- Logs top 10 patterns to help debug

## Troubleshooting

### Import Error

```
ImportError: Failed to import minimal_patterns Rust extension
```

**Solution**: Build the extension with `maturin develop --release`

### Build Errors

- Make sure you're in the dsgrid root directory, not `rust/`
- Check Rust version: `rustc --version` (need 1.70+)
- Try cleaning: `cargo clean` then rebuild

### Column Has Too Many Unique Values

```
RuntimeError: Column 'X' has 70000 unique values (max 65535 allowed)
```

**Solution**: The data needs pre-processing to reduce cardinality, or modify
the Rust code to use u32 instead of u16 (contact maintainer)

## Algorithm Details

### Overview of `lib.rs`

The Rust extension implements a **minimal closed pattern mining** algorithm optimized for describing combinations of categorical data in a minimal way, by only listing the dimension records necessary to identify regions of the data where full cross-joins are present.

Applied to a parquet file where each row represents a missing record combination from a dsgrid (or other) dataset, it discovers the smallest set of column-value constraints that explain groups of missing rows.

### Key Data Structures

1. **Dictionary Encoding**: Each column's unique values are mapped to `u16` codes (0-65535), enabling compact storage and fast comparisons.

2. **`EncodedDataset`**: A flattened row-major array storing encoded values. Access is `O(1)` via `data[row * num_cols + col]`.

3. **Inverted Index**: For each `(column, value)` pair, a `RoaringBitmap` stores which row IDs contain that value. This enables fast set intersection operations.

4. **`InternalPattern`**: Represents a pattern as:
   - `cols`: Which columns are constrained (e.g., `[0, 2]` for columns 0 and 2)
   - `vals`: The required values for those columns (encoded as u16)
   - `row_set`: A `RoaringBitmap` of matching row IDs
   - `closed`: Whether this pattern is closed

### Mathematical Principles

#### Closed Patterns

A pattern is **closed** if adding any additional column constraint would reduce coverage. Formally, pattern `P` with column set `C` and row set `R` is closed iff:

```
For all columns c ∉ C:
  |{distinct values of c in R}| = |domain(c)|
```

This means the pattern's rows contain ALL possible values for every unconstrained column. If any column has fewer distinct values in `R` than its domain, we could add that column to create a more specific pattern—so `P` isn't closed.

**Example**: If a pattern covers 100 rows and an unconstrained column "state" has 50 possible values, but only 30 distinct states appear in those 100 rows, the pattern is NOT closed (we could refine it by constraining "state").

#### Minimal Closed Patterns

A closed pattern is **minimal** if no proper subset of its constraints is also closed. These are the most general explanations—the root causes.

**Example**: If `{region=West}` is closed (covers all combinations for other dimensions), then `{region=West, year=2020}` would NOT be minimal even if closed, because a simpler pattern already explains it.

#### Why This Matters for Missing Data

When rows represent missing dimension combinations:
- A minimal closed pattern identifies the **smallest constraint** that explains a group of missing records
- If `{fuel_type=hydrogen}` is a minimal closed pattern covering 10,000 rows, it means ALL combinations involving hydrogen are missing—the root cause is "hydrogen data is missing", not individual records

### Algorithm Flow

1. **Two-Pass Parquet Reading**:
   - Pass 1: Build dictionaries (unique value → code mapping)
   - Pass 2: Encode all rows to u16 values

2. **Index Construction**: Build inverted index mapping `(column, value) → RoaringBitmap of row IDs`

3. **Level-wise Pattern Expansion**:
   ```
   Level 1: Generate all single-column patterns {col=val}
   For each level:
     - Check if pattern is closed
     - If closed AND no subset is closed → add to minimal_closed
     - If not closed → expand by adding one more column constraint
   ```

4. **Pivot Optimization**: When expanding a pattern:
   - If `|pattern_rows| < |dictionary_size|`: Iterate rows to find values (sparse)
   - Otherwise: Iterate dictionary values and intersect bitmaps (dense)

5. **Subset Pruning**: Before adding a closed pattern to results, check if any subset fingerprint is already closed. This enforces minimality.

### Complexity

- **Space**: O(rows × cols) for encoded data + O(unique_values) for bitmaps
- **Time**: Worst case exponential in columns, but pruning via closed/minimal checks drastically reduces the search space in practice

### Example

Input (missing records):
```
region  | fuel   | year
--------|--------|------
West    | coal   | 2020
West    | coal   | 2021
West    | gas    | 2020
West    | gas    | 2021
```

If these are the ONLY missing records and the domain of `year` is `{2020, 2021}` and `fuel` is `{coal, gas}`:
- Pattern `{region=West}` covers all 4 rows
- It's closed because all values of `fuel` and `year` appear in its rows
- It's minimal because no subset exists
- **Result**: `{region=West}` is the root cause—all West data is missing

## Future Enhancements

- [ ] Support for non-string column types
- [ ] Streaming pattern output for very large datasets
- [ ] Additional heuristics for pattern pruning
- [ ] Pattern visualization
