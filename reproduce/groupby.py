#!/usr/bin/env python3
"""Group-by benchmark: group by pk1, compute min/max of other columns, write to S3.

~10 rows per group on average.
Aggregates: MIN/MAX of pk2, pk3, str1, str2, str3, str4
"""

import time

from config import (
    OUTPUT_PREFIX,
    STRING_TABLE_A,
    cleanup_s3_path,
    duckdb_setup_s3,
    polars_storage_options,
)

DUCKDB_OUTPUT = f"{OUTPUT_PREFIX}/duckdb_groupby"
POLARS_OUTPUT = f"{OUTPUT_PREFIX}/polars_groupby.parquet"


# ============================================================
# DuckDB
# ============================================================
def run_duckdb():
    import duckdb

    con = duckdb.connect()
    duckdb_setup_s3(con)

    cleanup_s3_path(DUCKDB_OUTPUT)

    # === TIMED SECTION ===
    t0 = time.perf_counter()
    con.execute(f"""
        COPY (
            SELECT
                pk1,
                MIN(pk2) as min_pk2, MAX(pk2) as max_pk2,
                MIN(pk3) as min_pk3, MAX(pk3) as max_pk3,
                MIN(str1) as min_str1, MAX(str1) as max_str1,
                MIN(str2) as min_str2, MAX(str2) as max_str2,
                MIN(str3) as min_str3, MAX(str3) as max_str3,
                MIN(str4) as min_str4, MAX(str4) as max_str4,
                COUNT(*) as row_count
            FROM read_parquet('{STRING_TABLE_A}')
            GROUP BY pk1
        )
        TO '{DUCKDB_OUTPUT}'
        (FORMAT PARQUET, PER_THREAD_OUTPUT, ROW_GROUP_SIZE 300000);
    """)
    elapsed = time.perf_counter() - t0
    # === END TIMED SECTION ===

    con.close()
    return elapsed


# ============================================================
# Polars
# ============================================================
def run_polars():
    import polars as pl

    storage_opts = polars_storage_options()
    lf = pl.scan_parquet(STRING_TABLE_A, storage_options=storage_opts)

    # Build min/max aggregations for each column
    agg_cols = ["pk2", "pk3", "str1", "str2", "str3", "str4"]
    aggs = []
    for col in agg_cols:
        aggs.append(pl.col(col).min().alias(f"min_{col}"))
        aggs.append(pl.col(col).max().alias(f"max_{col}"))
    aggs.append(pl.len().alias("row_count"))

    cleanup_s3_path(POLARS_OUTPUT)

    # === TIMED SECTION ===
    t0 = time.perf_counter()
    (
        lf.group_by("pk1")
        .agg(aggs)
        .sink_parquet(
            POLARS_OUTPUT, storage_options=storage_opts, row_group_size=300_000
        )
    )
    elapsed = time.perf_counter() - t0
    # === END TIMED SECTION ===

    return elapsed


if __name__ == "__main__":
    print(f"Table: {STRING_TABLE_A}")
    print(f"DuckDB: {run_duckdb():.3f}s")
    print(f"Polars: {run_polars():.3f}s")
