#!/usr/bin/env python3
"""Compare DuckDB S3 access: HTTPFS extension vs fsspec/s3fs.

HTTPFS requires installing the extension (not possible in airgapped environments).
fsspec/s3fs uses Python's filesystem abstraction registered with DuckDB.

Reference: https://github.com/pydiverse/pydiverse.pipedag
"""

import time

import duckdb
import fsspec

from config import (
    ACCESS_KEY,
    BENCH_OUT_S3_PREFIX,
    ENDPOINT_URL,
    OUTPUT_PREFIX,
    REGION,
    SECRET_KEY,
    STRING_TABLE_A,
    STRING_TABLE_B,
    TPCH_PATHS,
    cleanup_s3_path,
)

# Output paths for this benchmark
HTTPFS_OUTPUT = f"{OUTPUT_PREFIX}/httpfs_join"
FSSPEC_OUTPUT = f"{OUTPUT_PREFIX}/fsspec_join"


def setup_duckdb_httpfs(con: duckdb.DuckDBPyConnection) -> None:
    """Setup DuckDB with HTTPFS extension (traditional approach)."""
    # Check if httpfs extension is already installed (e.g., via conda package)
    installed = con.execute(
        "SELECT installed FROM duckdb_extensions() WHERE extension_name = 'httpfs'"
    ).fetchone()

    if installed and installed[0]:
        con.execute("LOAD httpfs;")
    else:
        con.execute("INSTALL httpfs;")
        con.execute("LOAD httpfs;")

    endpoint = ENDPOINT_URL.replace("http://", "").replace("https://", "")
    secret_opts = ["TYPE s3"]
    secret_opts.append(f"KEY_ID '{ACCESS_KEY}'")
    secret_opts.append(f"SECRET '{SECRET_KEY}'")
    secret_opts.append(f"ENDPOINT '{endpoint}'")
    if ENDPOINT_URL.startswith("http://"):
        secret_opts.append("USE_SSL false")
    secret_opts.append("URL_STYLE path")
    secret_opts.append(f"REGION '{REGION}'")

    con.execute(f"CREATE SECRET ({', '.join(secret_opts)});")


def setup_duckdb_fsspec(con: duckdb.DuckDBPyConnection) -> None:
    """Setup DuckDB with fsspec/s3fs (no extension needed).

    Based on pydiverse.pipedag's approach for airgapped environments.
    """
    # Configure fsspec's global config for S3
    from fsspec.config import conf

    if "s3" not in conf and ENDPOINT_URL:
        conf.setdefault("s3", {})
        conf["s3"].update(
            {
                "key": ACCESS_KEY,
                "secret": SECRET_KEY,
                "client_kwargs": {
                    "endpoint_url": ENDPOINT_URL,
                    "region_name": REGION,
                },
                "config_kwargs": {
                    "connect_timeout": 3,
                    "read_timeout": 5,
                    "retries": {"max_attempts": 2, "mode": "standard"},
                    "s3": {"addressing_style": "path"},  # MinIO needs path-style
                },
            }
        )

    # Register the s3 filesystem with DuckDB
    fs = fsspec.filesystem("s3")
    con.register_filesystem(fs)


# ============================================================
# Join benchmark with HTTPFS
# ============================================================
def run_httpfs_join() -> float:
    con = duckdb.connect()
    setup_duckdb_httpfs(con)

    cleanup_s3_path(HTTPFS_OUTPUT)

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
        TO '{HTTPFS_OUTPUT}'
        (FORMAT PARQUET, PER_THREAD_OUTPUT, ROW_GROUP_SIZE 300000);
    """)
    elapsed = time.perf_counter() - t0
    # === END TIMED SECTION ===

    con.close()
    return elapsed


# ============================================================
# Join benchmark with fsspec/s3fs
# ============================================================
def run_fsspec_join() -> float:
    con = duckdb.connect()
    setup_duckdb_fsspec(con)

    cleanup_s3_path(FSSPEC_OUTPUT)

    # === TIMED SECTION ===
    t0 = time.perf_counter()
    con.execute(f"""
        COPY (
            SELECT
                a.pk1, a.pk2, a.pk3,
                a.str1 as a_str1, a.str2 as a_str2, a.str3 as a_str3, a.str4 as a_str4,
                b.str1 as b_str1, b.str2 as b_str2, b.str3 as b_str3, b.str4 as b_str4
            FROM read_parquet('s3://{STRING_TABLE_A.replace("s3://", "")}') a
            INNER JOIN read_parquet('s3://{STRING_TABLE_B.replace("s3://", "")}') b
                ON a.pk1 = b.pk1 AND a.pk2 = b.pk2 AND a.pk3 = b.pk3
        )
        TO 's3://{FSSPEC_OUTPUT.replace("s3://", "")}'
        (FORMAT PARQUET, PER_THREAD_OUTPUT, ROW_GROUP_SIZE 300000);
    """)
    elapsed = time.perf_counter() - t0
    # === END TIMED SECTION ===

    con.close()
    return elapsed


# ============================================================
# Count benchmark (read-only)
# ============================================================
def run_httpfs_count() -> float:
    con = duckdb.connect()
    setup_duckdb_httpfs(con)

    # === TIMED SECTION ===
    t0 = time.perf_counter()
    result = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet({TPCH_PATHS})
    """).fetchone()
    elapsed = time.perf_counter() - t0
    # === END TIMED SECTION ===

    con.close()
    return elapsed, result[0]


def run_fsspec_count() -> float:
    con = duckdb.connect()
    setup_duckdb_fsspec(con)

    paths = [p.replace("s3://", "") for p in TPCH_PATHS]

    # === TIMED SECTION ===
    t0 = time.perf_counter()
    result = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet({['s3://' + p for p in paths]})
    """).fetchone()
    elapsed = time.perf_counter() - t0
    # === END TIMED SECTION ===

    con.close()
    return elapsed, result[0]


def run_join_parametric(setup_fn, table_a: str, table_b: str, out_path: str) -> float:
    """Run join benchmark with configurable tables."""
    cleanup_s3_path(out_path)

    con = duckdb.connect()
    setup_fn(con)

    t0 = time.perf_counter()
    con.execute(f"""
        COPY (
            SELECT
                a.pk1, a.pk2, a.pk3,
                a.str1 as a_str1, a.str2 as a_str2, a.str3 as a_str3, a.str4 as a_str4,
                b.str1 as b_str1, b.str2 as b_str2, b.str3 as b_str3, b.str4 as b_str4
            FROM read_parquet('{table_a}') a
            INNER JOIN read_parquet('{table_b}') b
                ON a.pk1 = b.pk1 AND a.pk2 = b.pk2 AND a.pk3 = b.pk3
        )
        TO '{out_path}'
        (FORMAT PARQUET, PER_THREAD_OUTPUT, ROW_GROUP_SIZE 300000);
    """)
    elapsed = time.perf_counter() - t0

    con.close()
    return elapsed


def compare_sizes():
    """Compare HTTPFS vs fsspec overhead at different data sizes."""
    import os

    bucket = os.environ.get("BENCH_S3_BUCKET", "benchmark")
    prefix = os.environ.get("BENCH_S3_PREFIX", "tpch")
    out_prefix = os.environ.get("BENCH_OUT_S3_PREFIX", f"s3://{bucket}/outputs")

    print("=" * 70)
    print("HTTPFS vs fsspec/s3fs: Does overhead shrink with larger data?")
    print("=" * 70)

    results = []

    for rows_k in [1000, 5000, 20000]:
        table_a = f"s3://{bucket}/{prefix}/string_tables/table_a_{rows_k}k.parquet"
        table_b = f"s3://{bucket}/{prefix}/string_tables/table_b_{rows_k}k.parquet"

        print(f"\n--- {rows_k}K rows ---")

        # Run 3 times each
        httpfs_times = []
        fsspec_times = []

        for i in range(5):
            h = run_join_parametric(
                setup_duckdb_httpfs,
                table_a,
                table_b,
                f"{out_prefix}/bench_httpfs_{rows_k}k",
            )
            f = run_join_parametric(
                setup_duckdb_fsspec,
                table_a,
                table_b,
                f"{out_prefix}/bench_fsspec_{rows_k}k",
            )
            httpfs_times.append(h)
            fsspec_times.append(f)
            print(f"  Run {i+1}: HTTPFS={h:.3f}s, fsspec={f:.3f}s")

        httpfs_time = sorted(httpfs_times)[len(httpfs_times) // 2]
        fsspec_time = sorted(fsspec_times)[len(fsspec_times) // 2]
        overhead_pct = (fsspec_time / httpfs_time - 1) * 100

        results.append((rows_k, httpfs_time, fsspec_time, overhead_pct))
        print(f"  Median: HTTPFS={httpfs_time:.3f}s, fsspec={fsspec_time:.3f}s, overhead={overhead_pct:+.0f}%")

    print("\n" + "=" * 70)
    print("Summary")
    print("=" * 70)
    print(f"{'Rows':<10} {'HTTPFS':>10} {'fsspec':>10} {'Overhead':>12}")
    print("-" * 45)
    for rows_k, httpfs_time, fsspec_time, overhead_pct in results:
        print(f"{rows_k}K       {httpfs_time:>9.3f}s {fsspec_time:>9.3f}s {overhead_pct:>10.0f}%")

    print(f"\nOverhead change: {results[0][3]:.0f}% -> {results[1][3]:.0f}%")
    if results[1][3] < results[0][3]:
        print("Confirmed: fsspec overhead shrinks with larger data.")
    else:
        print("Note: overhead did not shrink as expected.")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--compare-sizes":
        compare_sizes()
    else:
        print("=" * 60)
        print("DuckDB S3 Access: HTTPFS vs fsspec/s3fs")
        print("=" * 60)
        print(f"\nString tables: {STRING_TABLE_A}")
        print(f"TPC-H paths: {len(TPCH_PATHS)} files")

        # Run count benchmark (read-only)
        print("\n--- COUNT benchmark (read-only) ---")
        httpfs_count_time, httpfs_count = run_httpfs_count()
        fsspec_count_time, fsspec_count = run_fsspec_count()
        print(f"HTTPFS: {httpfs_count_time:.3f}s (count={httpfs_count:,})")
        print(f"fsspec: {fsspec_count_time:.3f}s (count={fsspec_count:,})")
        print(f"Ratio:  {fsspec_count_time / httpfs_count_time:.2f}x")

        # Run join benchmark (read + write)
        print("\n--- JOIN benchmark (read + write) ---")
        httpfs_join_time = run_httpfs_join()
        fsspec_join_time = run_fsspec_join()
        print(f"HTTPFS: {httpfs_join_time:.3f}s")
        print(f"fsspec: {fsspec_join_time:.3f}s")
        print(f"Ratio:  {fsspec_join_time / httpfs_join_time:.2f}x")

        print("\n" + "=" * 60)
        print("Summary")
        print("=" * 60)
        print(f"{'Benchmark':<20} {'HTTPFS':>10} {'fsspec':>10} {'Ratio':>10}")
        print("-" * 50)
        print(
            f"{'count':<20} {httpfs_count_time:>9.3f}s {fsspec_count_time:>9.3f}s "
            f"{fsspec_count_time / httpfs_count_time:>9.2f}x"
        )
        print(
            f"{'join':<20} {httpfs_join_time:>9.3f}s {fsspec_join_time:>9.3f}s "
            f"{fsspec_join_time / httpfs_join_time:>9.2f}x"
        )
        print("\nNote: Ratio > 1.0 means fsspec is slower than HTTPFS")
        print("\nRun with --compare-sizes to compare 1M vs 5M rows")
