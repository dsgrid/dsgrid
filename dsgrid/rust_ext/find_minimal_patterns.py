"""Python wrapper for Rust-based pattern finding functionality."""

import csv
import logging
import shutil
from collections import defaultdict
from pathlib import Path

try:
    from dsgrid.minimal_patterns import Pattern, PatternConfig, find_minimal_patterns
except ImportError as e:
    msg = (
        "Failed to import minimal_patterns Rust extension. "
        "Make sure the package was built with maturin: `pip install -e .` or `maturin develop`"
    )
    raise ImportError() from e

logger = logging.getLogger(__name__)


def find_minimal_patterns_from_file(
    file_path: str | Path,
    max_depth: int = 0,
    prune_miss_empty: bool = True,
    ratio_threshold: float = 50.0,
    threads: int = 0,
    verbose: bool = False,
    output_dir: str | Path | None = "missing_associations",
) -> list[Pattern]:
    """Find minimal closed patterns in a Parquet file containing categorical data.

    This function analyzes a Parquet file to discover minimal closed patterns -
    the simplest column combinations that characterize complete subsets of your data.
    Patterns are grouped by their column combinations and written to CSV files.

    Parameters
    ----------
    file_path : str | Path
        Path to the input Parquet file
    max_depth : int, optional
        Maximum pattern size (number of columns). 0 = unlimited. Default: 0.
    prune_miss_empty : bool, optional
        Prune patterns with no matching rows (recommended: True). Default: True.
    ratio_threshold : float, optional
        Ratio threshold for pruning. Default: 50.0.
    threads : int, optional
        Number of threads to use (0 = use all available cores). Default: 0.
    verbose : bool, optional
        Enable verbose progress output. Default: False.
    output_dir : str | Path | None, optional
        Directory to write CSV files grouping patterns by column combinations.
        Each unique combination of columns produces a separate CSV file named
        ``<col1>__<col2>__...__<colN>.csv``. If None, no files are written.
        Default: "missing_associations".

    Returns
    -------
    list[Pattern]
        List of Pattern objects, each containing:

        - pattern_id : Unique identifier
        - columns : List of column names in the pattern
        - values : List of values for each column
        - num_rows : Number of rows matching this pattern

    Raises
    ------
    RuntimeError
        If there's an error reading the Parquet file or finding patterns.

    Examples
    --------
    >>> from dsgrid.rust_ext import find_patterns_from_parquet
    >>> patterns = find_patterns_from_parquet("missing_records.parquet", max_depth=3, verbose=True)
    >>> for p in patterns:
    ...     print(f"Pattern {p.pattern_id}: {p.columns} = {p.values} ({p.num_rows} rows)")
    """
    config = PatternConfig(
        max_depth=max_depth,
        prune_miss_empty=prune_miss_empty,
        ratio_threshold=ratio_threshold,
        threads=threads,
        verbose=verbose,
    )

    parquet_path_str = str(file_path)
    logger.info("Finding minimal closed patterns in %s", parquet_path_str)

    patterns = find_minimal_patterns(parquet_path_str, config)

    logger.info("Found %d minimal closed patterns", len(patterns))

    if output_dir is not None:
        _write_patterns_to_csv(patterns, output_dir)

    return patterns


def _write_patterns_to_csv(patterns: list[Pattern], output_dir: str | Path) -> None:
    """Write patterns to CSV files grouped by column combinations.

    Parameters
    ----------
    patterns : list[Pattern]
        List of Pattern objects to write.
    output_dir : str | Path
        Directory to write CSV files to.
    """
    output_path = Path(output_dir)
    if output_path.exists():
        shutil.rmtree(output_path)
    output_path.mkdir(parents=True)

    grouped: dict[tuple[str, ...], list[Pattern]] = defaultdict(list)
    for pattern in patterns:
        key = tuple(pattern.columns)
        grouped[key].append(pattern)

    for columns, group_patterns in grouped.items():
        filename = "__".join(columns) + ".csv"
        filepath = output_path / filename

        with open(filepath, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(list(columns))
            for pattern in group_patterns:
                writer.writerow(pattern.values)

        logger.info("Wrote %d patterns to %s", len(group_patterns), filepath)
