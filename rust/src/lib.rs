use std::collections::HashSet;
use std::fs::File;

use anyhow::{Context, Result};
use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use rayon::prelude::*;
use roaring::RoaringBitmap;
use ahash::{AHashMap, AHashSet};

use arrow2::io::parquet::read::{read_metadata, infer_schema, FileReader};
use arrow2::datatypes::{Schema, DataType};
use arrow2::array::{Utf8Array, Array, PrimitiveArray};

/// Helper to extract a string value from an array element at the given index.
/// Supports UTF8 arrays and integer arrays (converted to string).
fn get_string_value(arr: &dyn Array, idx: usize) -> Result<String> {
    match arr.data_type() {
        DataType::Utf8 => {
            let utf8 = arr.as_any().downcast_ref::<Utf8Array<i32>>()
                .context("Failed to downcast to Utf8Array<i32>")?;
            Ok(utf8.value(idx).to_string())
        }
        DataType::LargeUtf8 => {
            let utf8 = arr.as_any().downcast_ref::<Utf8Array<i64>>()
                .context("Failed to downcast to Utf8Array<i64>")?;
            Ok(utf8.value(idx).to_string())
        }
        DataType::Int8 => {
            let prim = arr.as_any().downcast_ref::<PrimitiveArray<i8>>()
                .context("Failed to downcast to PrimitiveArray<i8>")?;
            Ok(prim.value(idx).to_string())
        }
        DataType::Int16 => {
            let prim = arr.as_any().downcast_ref::<PrimitiveArray<i16>>()
                .context("Failed to downcast to PrimitiveArray<i16>")?;
            Ok(prim.value(idx).to_string())
        }
        DataType::Int32 => {
            let prim = arr.as_any().downcast_ref::<PrimitiveArray<i32>>()
                .context("Failed to downcast to PrimitiveArray<i32>")?;
            Ok(prim.value(idx).to_string())
        }
        DataType::Int64 => {
            let prim = arr.as_any().downcast_ref::<PrimitiveArray<i64>>()
                .context("Failed to downcast to PrimitiveArray<i64>")?;
            Ok(prim.value(idx).to_string())
        }
        DataType::UInt8 => {
            let prim = arr.as_any().downcast_ref::<PrimitiveArray<u8>>()
                .context("Failed to downcast to PrimitiveArray<u8>")?;
            Ok(prim.value(idx).to_string())
        }
        DataType::UInt16 => {
            let prim = arr.as_any().downcast_ref::<PrimitiveArray<u16>>()
                .context("Failed to downcast to PrimitiveArray<u16>")?;
            Ok(prim.value(idx).to_string())
        }
        DataType::UInt32 => {
            let prim = arr.as_any().downcast_ref::<PrimitiveArray<u32>>()
                .context("Failed to downcast to PrimitiveArray<u32>")?;
            Ok(prim.value(idx).to_string())
        }
        DataType::UInt64 => {
            let prim = arr.as_any().downcast_ref::<PrimitiveArray<u64>>()
                .context("Failed to downcast to PrimitiveArray<u64>")?;
            Ok(prim.value(idx).to_string())
        }
        dt => anyhow::bail!("Unsupported data type: {:?}", dt),
    }
}

/// A pattern discovered in the data
#[pyclass]
#[derive(Clone)]
pub struct Pattern {
    #[pyo3(get)]
    pub pattern_id: usize,
    #[pyo3(get)]
    pub columns: Vec<String>,
    #[pyo3(get)]
    pub values: Vec<String>,
    #[pyo3(get)]
    pub num_rows: u64,
}

#[pymethods]
impl Pattern {
    fn __repr__(&self) -> String {
        format!(
            "Pattern(id={}, columns={:?}, values={:?}, num_rows={})",
            self.pattern_id, self.columns, self.values, self.num_rows
        )
    }
}

#[derive(Clone)]
struct InternalPattern {
    cols: Vec<usize>,
    vals: Vec<u16>,
    row_set: RoaringBitmap,
    closed: bool,
}

/// Configuration for pattern finding
#[pyclass]
#[derive(Clone)]
pub struct PatternConfig {
    #[pyo3(get, set)]
    pub max_depth: usize,
    #[pyo3(get, set)]
    pub only_missing_values: bool,
    #[pyo3(get, set)]
    pub prune_miss_empty: bool,
    #[pyo3(get, set)]
    pub ratio_threshold: f64,
    #[pyo3(get, set)]
    pub threads: usize,
    #[pyo3(get, set)]
    pub verbose: bool,
}

#[pymethods]
impl PatternConfig {
    #[new]
    #[pyo3(signature = (max_depth=0, only_missing_values=false, prune_miss_empty=true, ratio_threshold=50.0, threads=0, verbose=false))]
    fn new(
        max_depth: usize,
        only_missing_values: bool,
        prune_miss_empty: bool,
        ratio_threshold: f64,
        threads: usize,
        verbose: bool,
    ) -> Self {
        PatternConfig {
            max_depth,
            only_missing_values,
            prune_miss_empty,
            ratio_threshold,
            threads,
            verbose,
        }
    }
}

/// Read and encode a Parquet file in two streaming passes.
fn read_and_encode_parquet(
    path: &str,
) -> Result<(Vec<Vec<u16>>, Vec<Vec<String>>, Vec<String>)> {
    let mut f = File::open(path).with_context(|| format!("Opening parquet file {}", path))?;
    let metadata = read_metadata(&mut f).context("Reading parquet metadata")?;
    let schema: Schema = infer_schema(&metadata).context("Inferring schema")?;

    // Check column types: allow UTF8 and integers, reject floats
    for field in &schema.fields {
        match &field.data_type {
            DataType::Utf8 | DataType::LargeUtf8 => {}
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
            | DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {}
            DataType::Float32 | DataType::Float64 => {
                anyhow::bail!(
                    "Float column '{}' found (dtype {:?}). Float columns are not supported.",
                    field.name,
                    field.data_type
                );
            }
            _ => {
                anyhow::bail!(
                    "Unsupported column type '{}' found (dtype {:?}). Only UTF8 and integer columns are supported.",
                    field.name,
                    field.data_type
                );
            }
        }
    }

    let d = schema.fields.len();
    let col_names: Vec<String> = schema.fields.iter().map(|f| f.name.clone()).collect();

    // PASS 1: Build dictionaries
    eprintln!("  Pass 1: Building dictionaries for {}", path);
    let mut dicts: Vec<Vec<String>> = vec![Vec::new(); d];
    let mut maps: Vec<AHashMap<String, u16>> = (0..d).map(|_| AHashMap::new()).collect();

    let row_groups = metadata.row_groups.clone();
    let reader = FileReader::new(f, row_groups.clone(), schema.clone(), None, None, None);

    for chunk_res in reader {
        let chunk = chunk_res.context("Reading parquet chunk in pass 1")?;
        let arrays = chunk.arrays();
        for (ci, arr) in arrays.iter().enumerate() {
            let arr_len = arr.len();
            for row_idx in 0..arr_len {
                let val = if arr.is_null(row_idx) {
                    String::new()
                } else {
                    get_string_value(arr.as_ref(), row_idx)
                        .with_context(|| format!("Getting string value for column '{}' in pass 1", col_names[ci]))?
                };
                if !maps[ci].contains_key(&val) {
                    let code = dicts[ci].len();
                    if code >= 65536 {
                        anyhow::bail!(
                            "Column '{}' has {} unique values (max 65535 allowed)",
                            col_names[ci],
                            code + 1
                        );
                    }
                    dicts[ci].push(val.clone());
                    maps[ci].insert(val, code as u16);
                }
            }
        }
    }

    eprintln!(
        "  Dictionary sizes: {:?}",
        dicts.iter().map(|d| d.len()).collect::<Vec<_>>()
    );

    // PASS 2: Encode to u16
    eprintln!("  Pass 2: Encoding to u16 for {}", path);
    let mut f2 = File::open(path).with_context(|| format!("Re-opening parquet file {}", path))?;
    let metadata2 = read_metadata(&mut f2).context("Re-reading parquet metadata")?;
    let row_groups2 = metadata2.row_groups.clone();
    let reader2 = FileReader::new(f2, row_groups2, schema.clone(), None, None, None);

    let mut encoded: Vec<Vec<u16>> = Vec::new();

    for chunk_res in reader2 {
        let chunk = chunk_res.context("Reading parquet chunk in pass 2")?;
        let arrays = chunk.arrays();
        let chunk_len = arrays[0].len();

        for row_idx in 0..chunk_len {
            let mut row = Vec::with_capacity(d);
            for (ci, arr) in arrays.iter().enumerate() {
                let val = if arr.is_null(row_idx) {
                    String::new()
                } else {
                    get_string_value(arr.as_ref(), row_idx)
                        .with_context(|| format!("Getting string value for column '{}' in pass 2", col_names[ci]))?
                };
                let code = *maps[ci].get(&val).unwrap();
                row.push(code);
            }
            encoded.push(row);
        }
    }

    eprintln!("  Encoded {} rows", encoded.len());
    Ok((encoded, dicts, col_names))
}

/// Build inverted index
fn build_index(
    encoded: &[Vec<u16>],
    d: usize,
    dicts: &[Vec<String>],
) -> Vec<Vec<RoaringBitmap>> {
    let mut index: Vec<Vec<RoaringBitmap>> = (0..d)
        .map(|c| vec![RoaringBitmap::new(); dicts[c].len()])
        .collect();
    for (rid, row) in encoded.iter().enumerate() {
        for (c, &val_code) in row.iter().enumerate() {
            index[c][val_code as usize].insert(rid as u32);
        }
    }
    index
}

/// Check if a pattern is closed
fn is_pattern_closed(
    pattern: &InternalPattern,
    encoded_data: &[Vec<u16>],
    dict_sizes: &[usize],
) -> bool {
    let pattern_cols_set: AHashSet<usize> = pattern.cols.iter().copied().collect();
    let remaining_cols: Vec<usize> = (0..dict_sizes.len())
        .filter(|c| !pattern_cols_set.contains(c))
        .collect();

    if remaining_cols.is_empty() {
        return !pattern.row_set.is_empty();
    }

    let expected_combinations: u64 = remaining_cols
        .iter()
        .map(|&c| dict_sizes[c] as u64)
        .product();

    let mut actual_combinations = AHashSet::new();
    for row_id in pattern.row_set.iter() {
        let row = &encoded_data[row_id as usize];
        let combo: Vec<u16> = remaining_cols.iter().map(|&c| row[c]).collect();
        actual_combinations.insert(combo);
    }

    actual_combinations.len() as u64 == expected_combinations
}

/// Generate pattern fingerprint
fn pattern_fingerprint(cols: &[usize], vals: &[u16]) -> String {
    cols.iter()
        .enumerate()
        .map(|(i, col)| format!("{}={}", col, vals[i]))
        .collect::<Vec<_>>()
        .join("|")
}

/// Generate subset fingerprints
fn generate_subset_fingerprints(cols: &[usize], vals: &[u16]) -> Vec<String> {
    let n = cols.len();
    let mut fps = Vec::new();
    for subset_size in 1..n {
        let mut mask: u64 = (1u64 << subset_size) - 1;
        while mask < (1u64 << n) {
            let mut parts = Vec::with_capacity(subset_size);
            for i in 0..n {
                if (mask >> i) & 1 == 1 {
                    parts.push(format!("{}={}", cols[i], vals[i]));
                }
            }
            fps.push(parts.join("|"));
            let u = mask & (!mask + 1);
            let v = mask + u;
            let y = v + (((v ^ mask) / u) >> 2);
            mask = if y >= (1u64 << n) { 1u64 << n } else { y };
        }
    }
    fps
}

/// Check if pattern should be expanded
fn should_expand(p: &InternalPattern, config: &PatternConfig) -> bool {
    if p.closed {
        return false;
    }
    if config.max_depth > 0 && p.cols.len() >= config.max_depth {
        return false;
    }
    if config.prune_miss_empty && p.row_set.is_empty() {
        return false;
    }
    true
}

/// Expand a single pattern
fn expand_one(
    pat: &InternalPattern,
    index_data: &[Vec<RoaringBitmap>],
    d: usize,
    config: &PatternConfig,
    value_sets: Option<&[AHashSet<u16>]>,
) -> Vec<InternalPattern> {
    if !should_expand(pat, config) {
        return Vec::new();
    }
    let last_col = *pat.cols.last().unwrap();
    let mut children = Vec::new();

    for new_col in (last_col + 1)..d {
        if pat.cols.contains(&new_col) {
            continue;
        }
        let candidate_values: Vec<usize> = if config.only_missing_values {
            if let Some(val_sets) = value_sets {
                val_sets[new_col].iter().map(|&v| v as usize).collect()
            } else {
                (0..index_data[new_col].len()).collect()
            }
        } else {
            (0..index_data[new_col].len()).collect()
        };

        for new_val_code in candidate_values {
            let mut new_row_set = pat.row_set.clone();
            new_row_set &= &index_data[new_col][new_val_code];

            if new_row_set.is_empty() {
                continue;
            }

            let mut new_cols = pat.cols.clone();
            new_cols.push(new_col);
            let mut new_vals = pat.vals.clone();
            new_vals.push(new_val_code as u16);

            children.push(InternalPattern {
                cols: new_cols,
                vals: new_vals,
                row_set: new_row_set,
                closed: false,
            });
        }
    }
    children
}

/// Find minimal closed patterns in a Parquet file
#[pyfunction]
#[pyo3(signature = (input_path, config=None))]
fn find_minimal_patterns(
    input_path: String,
    config: Option<PatternConfig>,
) -> PyResult<Vec<Pattern>> {
    let config = config.unwrap_or_else(|| PatternConfig::new(0, false, true, 50.0, 0, false));

    if config.threads > 0 {
        rayon::ThreadPoolBuilder::new()
            .num_threads(config.threads)
            .build_global()
            .map_err(|e| PyRuntimeError::new_err(format!("Setting Rayon thread pool: {}", e)))?;
    }

    eprintln!("Reading and encoding input parquet: {}", input_path);
    let (encoded_data, dicts, columns) = read_and_encode_parquet(&input_path)
        .map_err(|e| PyRuntimeError::new_err(format!("Reading parquet: {}", e)))?;

    let d = columns.len();
    eprintln!("Columns ({}): {:?}", d, columns);
    eprintln!("Input rows: {}", encoded_data.len());

    let dict_sizes: Vec<usize> = dicts.iter().map(|d| d.len()).collect();
    eprintln!("Dictionary sizes: {:?}", dict_sizes);

    eprintln!("Building data index...");
    let index_data = build_index(&encoded_data, d, &dicts);

    let value_sets: Option<Vec<AHashSet<u16>>> = if config.only_missing_values {
        let mut mv = Vec::with_capacity(d);
        for col in 0..d {
            let mut hs = AHashSet::new();
            for (val_code, bm) in index_data[col].iter().enumerate() {
                if !bm.is_empty() {
                    hs.insert(val_code as u16);
                }
            }
            mv.push(hs);
        }
        Some(mv)
    } else {
        None
    };

    eprintln!("Finished building index.");

    let mut level: Vec<InternalPattern> = Vec::new();
    let mut minimal_closed: Vec<InternalPattern> = Vec::new();
    let mut closed_fingerprints: HashSet<String> = HashSet::new();

    eprintln!("Generating single-column patterns...");
    for col in 0..d {
        let candidate_values: Vec<usize> = if config.only_missing_values {
            if let Some(vs) = &value_sets {
                vs[col].iter().map(|&v| v as usize).collect()
            } else {
                (0..index_data[col].len()).collect()
            }
        } else {
            (0..index_data[col].len()).collect()
        };

        for val_code in candidate_values {
            let row_set = index_data[col][val_code].clone();
            if row_set.is_empty() {
                continue;
            }

            let mut pat = InternalPattern {
                cols: vec![col],
                vals: vec![val_code as u16],
                row_set,
                closed: false,
            };

            let is_closed = is_pattern_closed(&pat, &encoded_data, &dict_sizes);
            pat.closed = is_closed;

            if is_closed {
                let fp = pattern_fingerprint(&pat.cols, &pat.vals);
                closed_fingerprints.insert(fp.clone());
                minimal_closed.push(pat);
            } else {
                level.push(pat);
            }
        }
    }

    eprintln!(
        "Generated {} single-column patterns ({} closed, {} to expand)",
        minimal_closed.len() + level.len(),
        minimal_closed.len(),
        level.len()
    );

    let mut iteration = 0usize;
    while !level.is_empty() {
        iteration += 1;
        if config.verbose {
            eprintln!(
                "Iteration {}: expanding {} patterns (stored minimal closed so far: {})",
                iteration,
                level.len(),
                minimal_closed.len()
            );
        }

        let expanded: Vec<InternalPattern> = level
            .par_iter()
            .flat_map(|pat| {
                expand_one(
                    pat,
                    &index_data,
                    d,
                    &config,
                    value_sets.as_ref().map(|v| &v[..]),
                )
            })
            .collect();

        if expanded.is_empty() {
            break;
        }

        eprintln!(
            "  Expanded to {} patterns, checking which are closed...",
            expanded.len()
        );

        let mut next_level: Vec<InternalPattern> = Vec::new();

        for mut p in expanded {
            let is_closed = is_pattern_closed(&p, &encoded_data, &dict_sizes);
            p.closed = is_closed;

            if is_closed {
                let subset_fps = generate_subset_fingerprints(&p.cols, &p.vals);
                if subset_fps.iter().any(|sf| closed_fingerprints.contains(sf)) {
                    continue;
                }
                let fp = pattern_fingerprint(&p.cols, &p.vals);
                closed_fingerprints.insert(fp);
                minimal_closed.push(p);
            } else {
                next_level.push(p);
            }
        }
        level = next_level;
    }

    eprintln!(
        "Completed. Found {} minimal closed patterns.",
        minimal_closed.len()
    );

    // Convert to Python-friendly format
    let result: Vec<Pattern> = minimal_closed
        .iter()
        .enumerate()
        .map(|(i, p)| {
            let col_names: Vec<String> = p.cols.iter().map(|&c| columns[c].clone()).collect();
            let val_strings: Vec<String> = p
                .cols
                .iter()
                .enumerate()
                .map(|(idx, &col)| dicts[col][p.vals[idx] as usize].clone())
                .collect();

            Pattern {
                pattern_id: i + 1,
                columns: col_names,
                values: val_strings,
                num_rows: p.row_set.len(),
            }
        })
        .collect();

    Ok(result)
}

/// Python module
#[pymodule]
fn minimal_patterns(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Pattern>()?;
    m.add_class::<PatternConfig>()?;
    m.add_function(wrap_pyfunction!(find_minimal_patterns, m)?)?;
    Ok(())
}
