# dsgrid Rust Extension - Pattern Analysis

This directory contains the Rust extension for high-performance pattern analysis in dsgrid.

## What It Does

The Rust extension provides fast analysis of missing dimension record patterns using the "minimal closed patterns" algorithm. This helps identify root causes when datasets have missing required dimension records.

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

## Future Enhancements

- [ ] Support for non-string column types
- [ ] Streaming pattern output for very large datasets
- [ ] Additional heuristics for pattern pruning
- [ ] Pattern visualization
