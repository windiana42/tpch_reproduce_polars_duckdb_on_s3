#!/usr/bin/env python3
"""Join benchmark: inner join two tables on 3 PK columns, write to S3.

Tables have 7 VARCHAR columns each (widths 4-17 chars).
Join on: pk1, pk2, pk3
"""

import time

from config import (
    OUTPUT_PREFIX,
    STRING_TABLE_A,
    STRING_TABLE_B,
    cleanup_s3_path,
    duckdb_setup_s3,
    polars_storage_options,
)

DUCKDB_OUTPUT = f"{OUTPUT_PREFIX}/duckdb_join"
POLARS_OUTPUT = f"{OUTPUT_PREFIX}/polars_join.parquet"


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
                a.pk1, a.pk2, a.pk3,
                a.str1 as a_str1, a.str2 as a_str2, a.str3 as a_str3, a.str4 as a_str4,
                b.str1 as b_str1, b.str2 as b_str2, b.str3 as b_str3, b.str4 as b_str4
            FROM read_parquet('{STRING_TABLE_A}') a
            INNER JOIN read_parquet('{STRING_TABLE_B}') b
                ON a.pk1 = b.pk1 AND a.pk2 = b.pk2 AND a.pk3 = b.pk3
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
    lf_a = pl.scan_parquet(STRING_TABLE_A, storage_options=storage_opts)
    lf_b = pl.scan_parquet(STRING_TABLE_B, storage_options=storage_opts)

    cleanup_s3_path(POLARS_OUTPUT)

    # === TIMED SECTION ===
    t0 = time.perf_counter()
    (
        lf_a.join(
            lf_b, on=["pk1", "pk2", "pk3"], how="inner", suffix="_b"
        ).sink_parquet(
            POLARS_OUTPUT, storage_options=storage_opts, row_group_size=300_000
        )
    )
    elapsed = time.perf_counter() - t0
    # === END TIMED SECTION ===

    return elapsed


if __name__ == "__main__":
    print(f"Table A: {STRING_TABLE_A}")
    print(f"Table B: {STRING_TABLE_B}")
    print(f"DuckDB: {run_duckdb():.3f}s")
    print(f"Polars: {run_polars():.3f}s")
