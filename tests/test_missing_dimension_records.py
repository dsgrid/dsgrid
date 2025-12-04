import pandas as pd
import numpy as np
from itertools import product, chain, combinations


## Functions
def get_all_combinations(lst: list[str], limit: int = 3) -> list[tuple[str]]:
    """Generate all combinations of a list up to a given length."""
    max_len = min(limit, len(lst))
    return chain.from_iterable(combinations(lst, i + 1) for i in range(max_len))


def generate_dim_records_and_record_counts(
    df: pd.DataFrame,
) -> tuple[dict[str, list], dict[str, int]]:
    """Generate dimension records and record counts from dataframe."""
    dim_records = {}
    dim_n_records = {}
    for col in df.columns:
        records = list(set(df[col]))
        dim_records[col] = records
        dim_n_records[col] = len(records)
    return dim_records, dim_n_records


def get_counts_per_dim(
    df: pd.DataFrame,
    dim_records: dict[str, list],
    dim_n_records: dict[str, int],
    missing_only: bool = False,
) -> pd.DataFrame:
    """Get counts of expected, actual, missing per dimension."""
    list_counts = []
    for dim in dim_records.keys():
        other_dims = [d for d in dim_records.keys() if d != dim]
        n_expected = np.prod([dim_n_records[d] for d in other_dims])
    dim_idx = pd.Index(dim_records[dim], name=dim)
    dim_count_df = df[dim].value_counts().reindex(dim_idx, fill_value=0)
    dim_count_df = dim_count_df.rename("actual").reset_index()

    assert all(
        dim_count_df["actual"] <= n_expected
    ), f"Dimension {dim} has more records than expected"
    dim_count_df["expected"] = n_expected
    # number of missing rows per dim value
    dim_count_df["missing"] = dim_count_df["expected"] - dim_count_df["actual"]
    dim_count_df = dim_count_df.rename(columns={dim: "records"}).assign(dimension=dim)
    if missing_only:
        dim_count_df = dim_count_df[dim_count_df["missing"] > 0]
    list_counts.append(dim_count_df)

    df_counts = pd.concat(list_counts, ignore_index=True)
    cols = ["dimension", "records", "expected", "actual", "missing"]
    df_counts = df_counts[cols]

    return df_counts


def get_counts(
    dim_list: list[tuple[str]],
    df: pd.DataFrame,
    dim_records: dict[str, list],
    dim_n_records: dict[str, int],
    missing_only: bool = False,
) -> pd.DataFrame:
    """Get counts of expected, actual, missing for a list of dimensions or dimension tuples."""
    list_counts = []
    for dims in dim_list:
        if isinstance(dims, str):
            dim_idx = pd.Index(dim_records[dims], name=dims)
            dim_lst = [
                dims,
            ]
        elif isinstance(dims, tuple):
            dim_combo = product(*[dim_records[d] for d in dims])
            dim_idx = pd.MultiIndex.from_tuples(dim_combo, names=dims)
            dim_lst = list(dims)
        else:
            msg = f"Unsupported format: {dims}"
            raise ValueError(msg)

        other_dims = [d for d in dim_records.keys() if d not in dim_lst]
        if other_dims:
            n_expected = np.prod([dim_n_records[d] for d in other_dims])
        else:
            n_expected = 1

        dim_count_df = df.groupby(dim_lst).size().reindex(dim_idx, fill_value=0)
        dim_count_df = dim_count_df.rename("actual").reset_index()

        assert all(
            dim_count_df["actual"] <= n_expected
        ), f"Dimension {dims} has more records than expected"
        dim_count_df["expected"] = n_expected
        # number of missing rows per dim value
        dim_count_df["missing"] = dim_count_df["expected"] - dim_count_df["actual"]
        if missing_only:
            dim_count_df = dim_count_df[dim_count_df["missing"] > 0]
        list_counts.append(dim_count_df)

    df_counts = pd.concat(list_counts, ignore_index=True)
    stats_cols = ["expected", "actual", "missing"]
    cols = [x for x in df_counts.columns if x not in stats_cols] + stats_cols
    df_counts = df_counts[cols]

    return df_counts


## Set up test data with missing rows
dct = {
    "dim1": ["A", "B"],
    "dim2": [1, 2],
    "dim3": ["X", "Y"],
}
df_orig = pd.DataFrame(list(product(*dct.values())), columns=dct.keys())
# missing_idx = [0, 1]
missing_idx = [0, df_orig.index[-1]]
df_missing_expected = df_orig.loc[missing_idx].copy()
df = df_orig.drop(index=missing_idx).reset_index(drop=True)
print("Input dataframe:")
print(df)

# Generate dimension records
dim_records, dim_n_records = generate_dim_records_and_record_counts(df)

# Get expected count for each dim in df
df_counts_by_dim = get_counts_per_dim(df, dim_records, dim_n_records)

# Get expected count for combinations of dims
dim_list = get_all_combinations(dim_records.keys())
""" dim_list =
[
    ("dim1",), ("dim2",), ("dim3",),
    ("dim1", "dim2"), ("dim1", "dim3"), ("dim2", "dim3"),
    ("dim1", "dim2", "dim3")
    ]
"""
df_counts = get_counts(dim_list, df, dim_records, dim_n_records)

# Get full missing rows
dim_list = [tuple(df.columns)]
df_counts_missing = get_counts(dim_list, df, dim_records, dim_n_records, missing_only=True)
print("\n [1] Missing rows:")
print(df_counts_missing)

df_missing = df_counts_missing[df.columns]
assert set(df_missing.itertuples(index=False)) == set(df_missing_expected.itertuples(index=False))

# Generalize missing rows by identifying trivial dimensions
print("\n [2] Reducing missing rows...")
mdim_records, mdim_n_records = generate_dim_records_and_record_counts(df_missing)
reduction_cand_list = get_all_combinations(mdim_records.keys(), limit=len(mdim_records) - 1)

reduction = []
for dims in reduction_cand_list:
    if isinstance(dims, str):
        dim_lst = [
            dims,
        ]
    elif isinstance(dims, tuple):
        dim_lst = list(dims)
    else:
        msg = f"Unsupported format: {dims}"
        raise ValueError(msg)

    other_dims = [d for d in mdim_records.keys() if d not in dim_lst]
    assert other_dims, "Should have other dimensions"

    dim_combo = product(*[dim_records[d] for d in dim_lst])
    if len(dim_lst) == 1:
        dim_combo = set(chain.from_iterable(dim_combo))
    else:
        dim_combo = set(dim_combo)

    cand_dims = []
    dct_cand = {
        key if len(key) > 1 else key[0]: df[other_dims]
        .sort_values(other_dims)
        .reset_index(drop=True)
        for key, df in df_missing.groupby(dim_lst)
    }
    assert dct_cand, "Should have candidate dictionary"

    found_candidate = False
    reduced_dfm = None
    if set(dct_cand.keys()) == dim_combo:
        if len(dct_cand) == 1:
            found_candidate = True
            reduced_dfm = list(dct_cand.values())[0]
        else:
            # Check if all candidate dfs are the same
            same_reduced_dfm = True
            for key, df in dct_cand.items():
                if reduced_dfm is None:
                    reduced_dfm = df
                else:
                    if not df.equals(reduced_dfm):
                        same_reduced_dfm = False
                        break
            if same_reduced_dfm:
                found_candidate = True

    if found_candidate:
        assert reduced_dfm is not None
        reduced_mising = {
            "missing": reduced_dfm,
            "applies_to": dim_lst,
        }
        reduction.append(reduced_mising)
        print(f"\nThese missing rows:\n{reduced_dfm}")
        print(f"Applies to dimensions:\n{dim_lst}\n")

if not reduction:
    print("No reduction found.")
