#!/usr/bin/env python3
"""DuckDB vs Polars OLAP benchmark (TPC-H lineitem) on S3.

Reproduces the plots and methodology described in the codecentric blog post, but with
datasets stored on S3 and optional write-heavy benchmarks back to S3.

Key features:
- generate: builds Parquet datasets directly to S3 using DuckDB tpch/dbgen, row groups 300k
- run: benchmarks DuckDB vs Polars on S3 (count / filter_write / copy_project)
- plot: recreates the time + peak RSS delta plots, including overlay plots

S3 auth:
- DuckDB: httpfs + CREATE SECRET (TYPE s3, PROVIDER credential_chain[, REGION ...])
- Polars: uses default env/instance-role creds or explicit storage_options

NOTE: Generating sf=640 and copying to multi-TB stress sets can be expensive.
"""

from __future__ import annotations

import argparse
import multiprocessing as mp
import os
import shutil
import subprocess
import tempfile
import threading
import time
import uuid
from collections.abc import Sequence
from datetime import date
from pathlib import Path
from typing import Any

import boto3
import duckdb
import matplotlib.pyplot as plt
import pandas as pd
import polars as pl
import psutil

# -----------------------------
# Benchmark configuration
# -----------------------------
# Defaults match the codecentric blog post, but can be overridden via environment variables.
# For ~100MB files: BENCH_NORMAL_SF="0.1 0.2" BENCH_STRESS_SMALL_BASE_SF=0.1
# For ~1-5GB files: BENCH_NORMAL_SF="1 2 5" BENCH_STRESS_SMALL_BASE_SF=1 BENCH_STRESS_BIG_BASE_SF=5


def _parse_env_list(key: str, default: list[float]) -> list[float]:
    """Parse space-separated float list from env var, or return default."""
    val = os.environ.get(key)
    if not val:
        return default
    return [float(x) for x in val.split()]


def _parse_env_float(key: str, default: float) -> float:
    """Parse float from env var, or return default."""
    val = os.environ.get(key)
    if not val:
        return default
    return float(val)


def _parse_env_int(key: str, default: int) -> int:
    """Parse int from env var, or return default."""
    val = os.environ.get(key)
    if not val:
        return default
    return int(val)


# Scale factors for normal single-file tests (env: BENCH_NORMAL_SF, space-separated)
# Original blog: [10, 20, 40, 80, 160, 320, 640] (~2GB to ~140GB)
DEFAULT_NORMAL_SF = [10, 20, 40, 80, 160]

# Stress-small: many small files of the same size
# Base SF determines file size, repeats determine file count
# Original blog: base_sf=10 (~2GB each), repeats=[1,2,4,8,18,36,72]
DEFAULT_STRESS_SMALL_BASE_SF = 10
DEFAULT_STRESS_SMALL_REPEATS = [1, 2, 4, 8, 18]

# Stress-big: few very large files
# Original blog: base_sf=640 (~140GB each), repeats=[1,2,4,8,14]
DEFAULT_STRESS_BIG_BASE_SF = 160
DEFAULT_STRESS_BIG_REPEATS = [1, 2, 4]


def get_normal_sf() -> list[float]:
    return _parse_env_list("BENCH_NORMAL_SF", DEFAULT_NORMAL_SF)


def get_stress_small_base_sf() -> float:
    return _parse_env_float("BENCH_STRESS_SMALL_BASE_SF", DEFAULT_STRESS_SMALL_BASE_SF)


def get_stress_small_repeats() -> list[int]:
    return [
        int(x)
        for x in _parse_env_list(
            "BENCH_STRESS_SMALL_REPEATS",
            [float(x) for x in DEFAULT_STRESS_SMALL_REPEATS],
        )
    ]


def get_stress_big_base_sf() -> float:
    return _parse_env_float("BENCH_STRESS_BIG_BASE_SF", DEFAULT_STRESS_BIG_BASE_SF)


def get_stress_big_repeats() -> list[int]:
    return [
        int(x)
        for x in _parse_env_list(
            "BENCH_STRESS_BIG_REPEATS", [float(x) for x in DEFAULT_STRESS_BIG_REPEATS]
        )
    ]


DEFAULT_ROW_GROUP_SIZE = 300_000
DEFAULT_ITERATIONS = 10
DEFAULT_SAMPLE_INTERVAL_S = 0.05

# -----------------------------
# String table configuration (for join/group-by benchmarks)
# -----------------------------
# Scales in thousands of rows (env: BENCH_STRING_SCALES, space-separated)
DEFAULT_STRING_SCALES = [10, 100, 1000, 10000]  # 10K to 1M rows
GROUPBY_CARDINALITY = 10  # Average rows per group


def get_string_scales() -> list[int]:
    return [
        int(x)
        for x in _parse_env_list(
            "BENCH_STRING_SCALES", [float(x) for x in DEFAULT_STRING_SCALES]
        )
    ]


def _resolve_sequence(values: list | None, default_fn) -> list:
    """Resolve CLI-provided values or fall back to env-configured/default values."""
    return default_fn() if values is None else values


# Query constants (as described in the post/repo)
SHIP_START = date(1994, 1, 1)
SHIP_END = date(1995, 1, 1)


# -----------------------------
# Utilities
# -----------------------------
def die(msg: str) -> None:
    raise SystemExit(msg)


def parse_s3_url(s3_url: str) -> tuple[str, str]:
    if not s3_url.startswith("s3://"):
        die(f"Expected s3:// URL, got: {s3_url}")
    rest = s3_url[len("s3://") :]
    parts = rest.split("/", 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ""
    return bucket, key


def join_s3(bucket: str, key: str) -> str:
    key = key.lstrip("/")
    return f"s3://{bucket}/{key}" if key else f"s3://{bucket}"


def ensure_prefix(prefix: str) -> str:
    # no leading slash, and no double slashes
    prefix = prefix.strip("/")
    return prefix


def aws_region(explicit: str | None) -> str | None:
    if explicit:
        return explicit
    return os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")


def aws_endpoint_url() -> str | None:
    return os.environ.get("AWS_ENDPOINT_URL")


def polars_storage_options_from_env(
    region: str | None, endpoint_url: str | None
) -> dict[str, str] | None:
    """If explicit env creds exist, return storage_options dict.

    Otherwise return None to let Polars use default provider chain (IAM role/profile).
    """
    ak = os.environ.get("AWS_ACCESS_KEY_ID")
    sk = os.environ.get("AWS_SECRET_ACCESS_KEY")
    if not (ak and sk):
        return None
    opts: dict[str, str] = {"aws_access_key_id": ak, "aws_secret_access_key": sk}
    st = os.environ.get("AWS_SESSION_TOKEN")
    if st:
        opts["aws_session_token"] = st
    if region:
        # Polars docs use "aws_region"
        opts["aws_region"] = region
    if endpoint_url:
        opts["endpoint_url"] = endpoint_url
        # MinIO/local S3 typically doesn't use SSL
        if endpoint_url.startswith("http://"):
            opts["aws_allow_http"] = "true"
    return opts


def s3_client(region: str | None) -> Any:
    sess = boto3.session.Session(region_name=region)
    endpoint_url = aws_endpoint_url()
    if endpoint_url:
        return sess.client("s3", endpoint_url=endpoint_url)
    return sess.client("s3")


def ensure_s3_bucket_exists(bucket: str, region: str | None) -> None:
    """Ensure bucket exists for local S3 endpoints (e.g. MinIO).

    On real AWS S3, buckets are often pre-provisioned and creating them implicitly is
    surprising, so we only auto-create when AWS_ENDPOINT_URL is set.

    This is important for the DuckDB CLI + httpfs path: when the target bucket does
    not exist, some DuckDB builds can terminate with SIGPIPE during COPY to S3.
    """
    endpoint_url = aws_endpoint_url()
    if not endpoint_url:
        return

    c = s3_client(region)
    try:
        c.head_bucket(Bucket=bucket)
        return
    except Exception:
        pass

    # MinIO typically accepts simple create_bucket without location constraints.
    print(f"[s3] creating bucket {bucket!r} at {endpoint_url}", flush=True)
    try:
        c.create_bucket(Bucket=bucket)
    except Exception:
        # Racy create or different semantics: re-check.
        c.head_bucket(Bucket=bucket)


def s3_list_objects(
    bucket: str, prefix: str, region: str | None
) -> list[dict[str, Any]]:
    c = s3_client(region)
    paginator = c.get_paginator("list_objects_v2")
    out: list[dict[str, Any]] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            out.append(obj)
    return out


def s3_sum_size_mb(bucket: str, prefix: str, region: str | None) -> float:
    objs = s3_list_objects(bucket, prefix, region)
    total = sum(int(o["Size"]) for o in objs)
    return total / (1024 * 1024)


def s3_copy_object(bucket: str, src_key: str, dst_key: str, region: str | None) -> None:
    c = s3_client(region)
    c.copy_object(
        Bucket=bucket, Key=dst_key, CopySource={"Bucket": bucket, "Key": src_key}
    )


def s3_delete_prefix(bucket: str, prefix: str, region: str | None) -> None:
    c = s3_client(region)
    objs = s3_list_objects(bucket, prefix, region)
    # batch delete 1000 at a time
    for i in range(0, len(objs), 1000):
        chunk = objs[i : i + 1000]
        c.delete_objects(
            Bucket=bucket, Delete={"Objects": [{"Key": o["Key"]} for o in chunk]}
        )


# -----------------------------
# DuckDB S3 setup + query
# -----------------------------
def duckdb_s3_method() -> str:
    """Get DuckDB S3 access method from environment.

    Returns 'fsspec' or 'httpfs'. Default is 'fsspec' for airgapped environments. Set
    BENCH_DUCKDB_S3_METHOD=httpfs to use the HTTPFS extension instead.
    """
    return os.environ.get("BENCH_DUCKDB_S3_METHOD", "fsspec").lower()


def duckdb_backend() -> str:
    """Get DuckDB execution backend.

    - python: use python-duckdb (in-process)
    - cli: run DuckDB via `duckdb` CLI (subprocess). This avoids in-process ABI issues
      between python-duckdb and conda-forge extension packages on some platforms.
    """
    return os.environ.get("BENCH_DUCKDB_BACKEND", "python").lower()


def _duckdb_cli_bin() -> str:
    p = os.environ.get("BENCH_DUCKDB_CLI") or shutil.which("duckdb")
    if not p:
        raise RuntimeError(
            "DuckDB CLI not found. Install conda-forge 'duckdb-cli' or set BENCH_DUCKDB_CLI."
        )
    return p


def _duckdb_secret_sql(region: str | None, endpoint_url: str | None) -> str:
    secret_opts = ["TYPE s3"]
    if endpoint_url:
        ak = os.environ.get("AWS_ACCESS_KEY_ID", "")
        sk = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
        secret_opts.append(f"KEY_ID '{ak}'")
        secret_opts.append(f"SECRET '{sk}'")
        endpoint_stripped = endpoint_url.replace("http://", "").replace("https://", "")
        secret_opts.append(f"ENDPOINT '{endpoint_stripped}'")
        if endpoint_url.startswith("http://"):
            secret_opts.append("USE_SSL false")
        secret_opts.append("URL_STYLE path")
    else:
        secret_opts.append("PROVIDER credential_chain")
    if region:
        secret_opts.append(f"REGION '{region}'")
    return f"CREATE SECRET ({', '.join(secret_opts)});"


def _duckdb_cli_script(
    *,
    sql_body: str,
    region: str | None,
    endpoint_url: str | None,
    threads: int | None,
    load_tpch: bool,
) -> str:
    # CLI backend only supports httpfs (fsspec is python-only)
    if duckdb_s3_method() != "httpfs":
        raise RuntimeError("DuckDB CLI backend requires BENCH_DUCKDB_S3_METHOD=httpfs")

    httpfs_path = _get_extension_path("httpfs")
    if not httpfs_path:
        raise RuntimeError(
            f"httpfs extension not found for DuckDB {duckdb.__version__} in this environment"
        )
    tpch_path = _get_extension_path("tpch") if load_tpch else None
    if load_tpch and not tpch_path:
        raise RuntimeError(
            f"tpch extension not found for DuckDB {duckdb.__version__} in this environment"
        )

    parts: list[str] = []
    if threads and threads > 0:
        parts.append(f"SET threads={int(threads)};")
    parts.append(f"LOAD '{httpfs_path}';")
    parts.append(_duckdb_secret_sql(region, endpoint_url))
    if load_tpch and tpch_path:
        parts.append(f"LOAD '{tpch_path}';")
    parts.append(sql_body.rstrip().rstrip(";") + ";")
    return "\n".join(parts) + "\n"


def _duckdb_cli_exec(
    *,
    sql_script: str,
    capture_stdout: bool,
    csv_no_header: bool,
) -> tuple[float, float, str]:
    """Run DuckDB CLI with a SQL script.

    Returns (time_s, peak_rss_mb, stdout_text).
    """
    args = [_duckdb_cli_bin(), "-unsigned", "-no-stdin"]
    if csv_no_header:
        args.extend(["-csv", "-noheader"])

    # Use an in-memory DB per invocation (fresh process already isolates state).
    args.append(":memory:")

    with tempfile.NamedTemporaryFile("w", suffix=".sql", delete=False) as f:
        f.write(sql_script)
        script_path = f.name

    try:
        args.extend(["-f", script_path])
        t0 = time.perf_counter()
        p = subprocess.Popen(
            args,
            stdout=subprocess.PIPE if capture_stdout else subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
        peak_rss = 0
        try:
            proc = psutil.Process(p.pid)
            while p.poll() is None:
                try:
                    rss = proc.memory_info().rss
                    if rss > peak_rss:
                        peak_rss = rss
                except Exception:
                    pass
                time.sleep(DEFAULT_SAMPLE_INTERVAL_S)
        except Exception:
            pass

        out, err = p.communicate()
        t1 = time.perf_counter()
        if p.returncode != 0:
            raise RuntimeError(
                f"DuckDB CLI failed (exit {p.returncode}). stderr:\n{(err or '').strip()}"
            )

        peak_mb = peak_rss / (1024 * 1024) if peak_rss else 0.0
        return (t1 - t0, peak_mb, out or "")
    finally:
        try:
            os.unlink(script_path)
        except OSError:
            pass


def duckdb_setup_s3(
    con: duckdb.DuckDBPyConnection,
    region: str | None,
    endpoint_url: str | None,
    verbose: bool = False,
) -> None:
    """Setup DuckDB for S3 access using either fsspec or httpfs.

    The method is controlled by BENCH_DUCKDB_S3_METHOD environment variable.
    - 'fsspec' (default): Uses fsspec/s3fs, works in airgapped environments
    - 'httpfs': Uses DuckDB's HTTPFS extension, requires network to install
    """
    method = duckdb_s3_method()

    if verbose:
        ak = os.environ.get("AWS_ACCESS_KEY_ID", "")
        sk = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
        env_endpoint = os.environ.get("AWS_ENDPOINT_URL", "")
        print(f"[S3 setup] method={method}", flush=True)
        print(f"[S3 setup] region={region}", flush=True)
        print(f"[S3 setup] endpoint_url param={endpoint_url}", flush=True)
        print(f"[S3 setup] AWS_ENDPOINT_URL env={env_endpoint}", flush=True)
        print(f"[S3 setup] AWS_ACCESS_KEY_ID={ak[:4]}...{ak[-4:] if len(ak) > 8 else ''}", flush=True)
        print(f"[S3 setup] AWS_SECRET_ACCESS_KEY={'*' * min(len(sk), 8)}", flush=True)

    if method == "fsspec":
        _duckdb_setup_s3_fsspec(con, region, endpoint_url, verbose=verbose)
    else:
        _duckdb_setup_s3_httpfs(con, region, endpoint_url, verbose=verbose)


def _clear_fsspec_s3_config() -> None:
    """Clear any existing fsspec S3 configuration to avoid pollution."""
    try:
        from fsspec.config import conf
        if "s3" in conf:
            conf.pop("s3")
    except ImportError:
        pass


def _duckdb_setup_s3_fsspec(
    con: duckdb.DuckDBPyConnection,
    region: str | None,
    endpoint_url: str | None,
    verbose: bool = False,
) -> None:
    """Setup DuckDB with fsspec/s3fs (no extension needed).

    Based on pydiverse.pipedag's approach for airgapped environments.
    Reference: https://github.com/pydiverse/pydiverse.pipedag
    """
    import fsspec
    from fsspec.config import conf

    # Clear any existing config to avoid pollution between runs
    _clear_fsspec_s3_config()

    # Configure fsspec's global config for S3
    conf.setdefault("s3", {})
    s3_conf: dict = {
        "key": os.environ.get("AWS_ACCESS_KEY_ID", ""),
        "secret": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
        "config_kwargs": {
            "connect_timeout": 3,
            "read_timeout": 5,
            "retries": {"max_attempts": 2, "mode": "standard"},
        },
    }
    if endpoint_url:
        s3_conf["client_kwargs"] = {"endpoint_url": endpoint_url}
        s3_conf["config_kwargs"]["s3"] = {"addressing_style": "path"}
    if region:
        s3_conf.setdefault("client_kwargs", {})["region_name"] = region
    conf["s3"].update(s3_conf)

    if verbose:
        print(f"[fsspec] configured s3 with endpoint={endpoint_url}, region={region}", flush=True)

    # Register the s3 filesystem with DuckDB
    fs = fsspec.filesystem("s3")
    con.register_filesystem(fs)
    if verbose:
        print("[fsspec] registered filesystem with DuckDB", flush=True)


def _get_extension_path(extension_name: str) -> str | None:
    """Get the path to a DuckDB extension from conda package.

    Args:
        extension_name: Name of the extension (e.g., "httpfs", "tpch")

    Returns the path if found, None otherwise.
    """
    import platform
    import sys

    version = duckdb.__version__

    # Determine platform string (matches conda package structure)
    machine = platform.machine()
    system = platform.system().lower()
    if system == "darwin":
        platform_str = f"osx_{machine}"
    elif system == "linux":
        # Linux uses amd64 not x86_64 in conda package paths
        if machine == "x86_64":
            platform_str = "linux_amd64"
        else:
            platform_str = f"linux_{machine}"
    else:
        platform_str = f"{system}_{machine}"

    # Check for extension in conda environment
    ext_path = f"{sys.prefix}/duckdb/extensions/v{version}/{platform_str}/{extension_name}.duckdb_extension"
    if os.path.exists(ext_path):
        return ext_path

    return None


def _get_httpfs_extension_path() -> str | None:
    """Get the path to httpfs extension from duckdb-extension-httpfs conda package."""
    return _get_extension_path("httpfs")


def duckdb_connect_for_s3(verbose: bool = False) -> duckdb.DuckDBPyConnection:
    """Create a DuckDB connection for S3 access.

    When using httpfs mode with extension from conda package, enables
    allow_unsigned_extensions (required for loading from non-default path).
    """
    method = duckdb_s3_method()
    config: dict[str, Any] = {}

    if method == "httpfs":
        ext_path = _get_httpfs_extension_path()
        if ext_path:
            # Loading from conda package requires unsigned extensions
            config["allow_unsigned_extensions"] = True
            if verbose:
                print("[duckdb] enabling allow_unsigned_extensions for conda package", flush=True)

    return duckdb.connect(database=":memory:", config=config)


def _duckdb_setup_s3_httpfs(
    con: duckdb.DuckDBPyConnection,
    region: str | None,
    endpoint_url: str | None,
    verbose: bool = False,
) -> None:
    """Setup DuckDB with HTTPFS extension.

    Tries to load from duckdb-extension-httpfs conda package first. Falls back to
    INSTALL httpfs if package not available.
    """
    # Clear any fsspec config to avoid polluting Polars S3 access
    _clear_fsspec_s3_config()

    # Load from conda package (duckdb-extension-httpfs)
    ext_path = _get_httpfs_extension_path()
    if verbose:
        print(f"[httpfs] DuckDB version: {duckdb.__version__}", flush=True)
        print(f"[httpfs] extension path: {ext_path}", flush=True)

    if not ext_path:
        raise RuntimeError(
            f"httpfs extension not found for DuckDB {duckdb.__version__}. "
            "Ensure duckdb-extension-httpfs conda package is installed and versions match."
        )

    if verbose:
        print("[httpfs] loading from conda package...", flush=True)
    con.execute(f"LOAD '{ext_path}';")
    if verbose:
        print("[httpfs] extension loaded", flush=True)

    # Build secret options
    secret_opts = ["TYPE s3"]

    if endpoint_url:
        # For MinIO/custom S3 endpoints, use explicit credentials instead of credential_chain
        ak = os.environ.get("AWS_ACCESS_KEY_ID", "")
        sk = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
        secret_opts.append(f"KEY_ID '{ak}'")
        secret_opts.append(f"SECRET '{sk}'")
        endpoint_stripped = endpoint_url.replace("http://", "").replace("https://", "")
        secret_opts.append(f"ENDPOINT '{endpoint_stripped}'")
        if endpoint_url.startswith("http://"):
            secret_opts.append("USE_SSL false")
        secret_opts.append("URL_STYLE path")  # MinIO uses path-style URLs
    else:
        secret_opts.append("PROVIDER credential_chain")

    if region:
        secret_opts.append(f"REGION '{region}'")

    if verbose:
        # Log secret options (mask sensitive values)
        safe_opts = [
            o if not o.startswith("KEY_ID") and not o.startswith("SECRET")
            else o.split("'")[0] + "'***'"
            for o in secret_opts
        ]
        print(f"[httpfs] CREATE SECRET options: {safe_opts}", flush=True)

    con.execute(f"CREATE SECRET ({', '.join(secret_opts)});")
    if verbose:
        print("[httpfs] secret created", flush=True)


def duckdb_setup_tpch(con: duckdb.DuckDBPyConnection, verbose: bool = False) -> None:
    """Load TPC-H extension from conda package.

    Uses duckdb-extension-tpch conda package instead of INSTALL to work in airgapped
    environments.
    """
    ext_path = _get_extension_path("tpch")
    if verbose:
        print(f"[tpch] extension path: {ext_path}", flush=True)

    if not ext_path:
        raise RuntimeError(
            f"tpch extension not found for DuckDB {duckdb.__version__}. "
            "Ensure duckdb-extension-tpch conda package is installed and versions match."
        )

    if verbose:
        print("[tpch] loading from conda package...", flush=True)
    con.execute(f"LOAD '{ext_path}';")
    if verbose:
        print("[tpch] extension loaded", flush=True)


def duckdb_set_threads(con: duckdb.DuckDBPyConnection, threads: int | None) -> None:
    if threads and threads > 0:
        con.execute(f"SET threads={int(threads)};")


def duckdb_sql_union_all_parquet(paths: list[str], where_sql: str | None) -> str:
    # Build UNION ALL subqueries like described in the post for stress tests.
    # Each subquery includes projection and optional filter.
    subs = []
    for p in paths:
        if where_sql:
            subs.append(
                "SELECT l_shipdate, l_discount, l_quantity "
                f"FROM read_parquet('{p}') {where_sql}"
            )
        else:
            subs.append(
                f"SELECT l_shipdate, l_discount, l_quantity FROM read_parquet('{p}')"
            )
    return " UNION ALL ".join(subs)


def duckdb_run_python(
    paths: list[str],
    mode: str,
    out_s3_prefix: str | None,
    row_group_size: int,
    threads: int | None,
    region: str | None,
    endpoint_url: str | None,
) -> dict[str, Any]:
    con = duckdb_connect_for_s3()
    duckdb_set_threads(con, threads)
    duckdb_setup_s3(con, region, endpoint_url)

    where_sql = (
        "WHERE l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01' "
        "AND l_discount BETWEEN 0.05 AND 0.07 "
        "AND l_quantity < 24"
    )

    # Setup done; start measuring only now.
    proc = psutil.Process(os.getpid())
    baseline_rss = proc.memory_info().rss
    peak_rss = baseline_rss
    stop = threading.Event()

    def sampler() -> None:
        nonlocal peak_rss
        while not stop.is_set():
            try:
                rss = proc.memory_info().rss
                if rss > peak_rss:
                    peak_rss = rss
            except Exception:
                pass
            time.sleep(DEFAULT_SAMPLE_INTERVAL_S)

    th = threading.Thread(target=sampler, daemon=True)
    th.start()

    t0 = time.perf_counter()
    result_row_count: int | None = None
    written_prefix: str | None = None

    if mode == "count":
        union_sql = duckdb_sql_union_all_parquet(paths, where_sql)
        sql = f"SELECT COUNT(*) FROM ({union_sql}) t;"
        result_row_count = int(con.execute(sql).fetchone()[0])

    elif mode == "filter_write":
        if not out_s3_prefix:
            die("filter_write requires --out-s3-prefix")
        union_sql = duckdb_sql_union_all_parquet(paths, where_sql)
        # Write filtered rows back to S3
        run_id = str(uuid.uuid4())
        written_prefix = out_s3_prefix.rstrip("/") + f"/duckdb/filter_write/{run_id}"
        sql = (
            "COPY ("
            f"SELECT * FROM ({union_sql}) t"
            ") TO "
            f"'{written_prefix}' "
            f"(FORMAT PARQUET, PER_THREAD_OUTPUT, ROW_GROUP_SIZE {row_group_size});"
        )
        con.execute(sql)

    elif mode == "copy_project":
        if not out_s3_prefix:
            die("copy_project requires --out-s3-prefix")
        union_sql = duckdb_sql_union_all_parquet(
            paths, where_sql=None
        )  # no filter => maximal output
        run_id = str(uuid.uuid4())
        written_prefix = out_s3_prefix.rstrip("/") + f"/duckdb/copy_project/{run_id}"
        sql = (
            "COPY ("
            f"SELECT * FROM ({union_sql}) t"
            ") TO "
            f"'{written_prefix}' "
            f"(FORMAT PARQUET, PER_THREAD_OUTPUT, ROW_GROUP_SIZE {row_group_size});"
        )
        con.execute(sql)

    else:
        die(f"Unknown mode: {mode}")

    t1 = time.perf_counter()
    stop.set()
    th.join(timeout=1.0)

    return {
        "row_count": result_row_count,
        "time_s": t1 - t0,
        "memory_mb": (peak_rss - baseline_rss) / (1024 * 1024),
        "written_prefix": written_prefix,
    }


def duckdb_run_cli(
    paths: list[str],
    mode: str,
    out_s3_prefix: str | None,
    row_group_size: int,
    threads: int | None,
    region: str | None,
    endpoint_url: str | None,
) -> dict[str, Any]:
    where_sql = (
        "WHERE l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01' "
        "AND l_discount BETWEEN 0.05 AND 0.07 "
        "AND l_quantity < 24"
    )

    result_row_count: int | None = None
    written_prefix: str | None = None
    csv_no_header = False
    sql_body: str

    if mode == "count":
        union_sql = duckdb_sql_union_all_parquet(paths, where_sql)
        sql_body = f"SELECT COUNT(*) FROM ({union_sql}) t"
        csv_no_header = True

    elif mode == "filter_write":
        if not out_s3_prefix:
            die("filter_write requires --out-s3-prefix")
        union_sql = duckdb_sql_union_all_parquet(paths, where_sql)
        run_id = str(uuid.uuid4())
        written_prefix = out_s3_prefix.rstrip("/") + f"/duckdb/filter_write/{run_id}"
        sql_body = (
            "COPY ("
            f"SELECT * FROM ({union_sql}) t"
            ") TO "
            f"'{written_prefix}' "
            f"(FORMAT PARQUET, PER_THREAD_OUTPUT, ROW_GROUP_SIZE {row_group_size})"
        )

    elif mode == "copy_project":
        if not out_s3_prefix:
            die("copy_project requires --out-s3-prefix")
        union_sql = duckdb_sql_union_all_parquet(paths, where_sql=None)
        run_id = str(uuid.uuid4())
        written_prefix = out_s3_prefix.rstrip("/") + f"/duckdb/copy_project/{run_id}"
        sql_body = (
            "COPY ("
            f"SELECT * FROM ({union_sql}) t"
            ") TO "
            f"'{written_prefix}' "
            f"(FORMAT PARQUET, PER_THREAD_OUTPUT, ROW_GROUP_SIZE {row_group_size})"
        )
    else:
        die(f"Unknown mode: {mode}")

    script = _duckdb_cli_script(
        sql_body=sql_body,
        region=region,
        endpoint_url=endpoint_url,
        threads=threads,
        load_tpch=False,
    )
    time_s, mem_mb, stdout = _duckdb_cli_exec(
        sql_script=script, capture_stdout=(mode == "count"), csv_no_header=csv_no_header
    )
    if mode == "count":
        last = stdout.strip().splitlines()[-1] if stdout.strip() else ""
        result_row_count = int(last.split(",")[0]) if last else 0

    return {
        "row_count": result_row_count,
        "time_s": time_s,
        "memory_mb": mem_mb,
        "written_prefix": written_prefix,
    }


def duckdb_run(
    paths: list[str],
    mode: str,
    out_s3_prefix: str | None,
    row_group_size: int,
    threads: int | None,
    region: str | None,
    endpoint_url: str | None,
) -> dict[str, Any]:
    backend = duckdb_backend()
    if backend == "cli":
        return duckdb_run_cli(
            paths=paths,
            mode=mode,
            out_s3_prefix=out_s3_prefix,
            row_group_size=row_group_size,
            threads=threads,
            region=region,
            endpoint_url=endpoint_url,
        )
    return duckdb_run_python(
        paths=paths,
        mode=mode,
        out_s3_prefix=out_s3_prefix,
        row_group_size=row_group_size,
        threads=threads,
        region=region,
        endpoint_url=endpoint_url,
    )


# -----------------------------
# Polars query
# -----------------------------
def polars_set_threads(threads: int | None) -> None:
    # Polars uses a global thread pool; easiest control is env var.
    # If user sets it, we respect it; otherwise optionally set here for the child process.
    if threads and threads > 0:
        os.environ["POLARS_MAX_THREADS"] = str(int(threads))


def polars_lazy_scan(
    paths: list[str], storage_options: dict[str, str] | None
) -> pl.LazyFrame:
    return pl.scan_parquet(paths, storage_options=storage_options)


def polars_run(
    paths: list[str],
    mode: str,
    out_s3_prefix: str | None,
    row_group_size: int,
    threads: int | None,
    storage_options: dict[str, str] | None,
    streaming: bool,
    sink_engine: str,
) -> dict[str, Any]:
    polars_set_threads(threads)

    # Build lazy plan (setup); start measuring only at execution time.
    lf = polars_lazy_scan(paths, storage_options=storage_options).select(
        ["l_shipdate", "l_discount", "l_quantity"]
    )

    filt = (
        (pl.col("l_shipdate") >= pl.lit(SHIP_START))
        & (pl.col("l_shipdate") < pl.lit(SHIP_END))
        & (pl.col("l_discount") >= pl.lit(0.05))
        & (pl.col("l_discount") <= pl.lit(0.07))
        & (pl.col("l_quantity") < pl.lit(24))
    )

    proc = psutil.Process(os.getpid())
    baseline_rss = proc.memory_info().rss
    peak_rss = baseline_rss
    stop = threading.Event()

    def sampler() -> None:
        nonlocal peak_rss
        while not stop.is_set():
            try:
                rss = proc.memory_info().rss
                if rss > peak_rss:
                    peak_rss = rss
            except Exception:
                pass
            time.sleep(DEFAULT_SAMPLE_INTERVAL_S)

    th = threading.Thread(target=sampler, daemon=True)
    th.start()

    t0 = time.perf_counter()
    result_row_count: int | None = None
    written_prefix: str | None = None

    if mode == "count":
        out = lf.filter(filt).select(pl.len().alias("cnt")).collect(streaming=streaming)
        result_row_count = int(out["cnt"][0])

    elif mode == "filter_write":
        if not out_s3_prefix:
            die("filter_write requires --out-s3-prefix")
        run_id = str(uuid.uuid4())
        out_path = out_s3_prefix.rstrip("/") + f"/polars/filter_write/{run_id}.parquet"
        written_prefix = out_path
        (
            lf.filter(filt).sink_parquet(
                out_path,
                storage_options=storage_options,
                row_group_size=row_group_size,
                engine=sink_engine,
            )
        )

    elif mode == "copy_project":
        if not out_s3_prefix:
            die("copy_project requires --out-s3-prefix")
        run_id = str(uuid.uuid4())
        out_path = out_s3_prefix.rstrip("/") + f"/polars/copy_project/{run_id}.parquet"
        written_prefix = out_path
        (
            lf.sink_parquet(
                out_path,
                storage_options=storage_options,
                row_group_size=row_group_size,
                engine=sink_engine,
            )
        )

    else:
        die(f"Unknown mode: {mode}")

    t1 = time.perf_counter()
    stop.set()
    th.join(timeout=1.0)

    return {
        "row_count": result_row_count,
        "time_s": t1 - t0,
        "memory_mb": (peak_rss - baseline_rss) / (1024 * 1024),
        "written_prefix": written_prefix,
    }


# -----------------------------
# Child-process worker (fresh process per run)
# -----------------------------
def worker_entry(conn, payload: dict[str, Any]) -> None:
    try:
        tool = payload["tool"]
        paths = payload["paths"]
        mode = payload["mode"]
        out_s3_prefix = payload.get("out_s3_prefix")
        row_group_size = int(payload["row_group_size"])
        threads = payload.get("threads")
        region = payload.get("region")
        endpoint_url = payload.get("endpoint_url")
        streaming = bool(payload.get("streaming", False))
        sink_engine = payload.get("sink_engine", "auto")

        if tool == "duckdb":
            res = duckdb_run(
                paths=paths,
                mode=mode,
                out_s3_prefix=out_s3_prefix,
                row_group_size=row_group_size,
                threads=threads,
                region=region,
                endpoint_url=endpoint_url,
            )
        elif tool == "polars":
            storage_options = payload.get("storage_options")
            res = polars_run(
                paths=paths,
                mode=mode,
                out_s3_prefix=out_s3_prefix,
                row_group_size=row_group_size,
                threads=threads,
                storage_options=storage_options,
                streaming=streaming,
                sink_engine=sink_engine,
            )
        else:
            raise ValueError(f"Unknown tool: {tool}")

        conn.send({"ok": True, "result": res})
    except Exception as e:
        conn.send({"ok": False, "error": repr(e)})
    finally:
        conn.close()


def run_in_fresh_process(payload: dict[str, Any]) -> dict[str, Any]:
    ctx = mp.get_context("spawn")
    parent_conn, child_conn = ctx.Pipe()
    p = ctx.Process(target=worker_entry, args=(child_conn, payload))
    p.start()
    msg = parent_conn.recv()
    p.join()

    if not msg.get("ok"):
        raise RuntimeError(f"Child failed: {msg.get('error')}")
    return msg["result"]


# -----------------------------
# Dataset layout on S3
# -----------------------------
def s3_dataset_key(prefix: str, test: str, name: str) -> str:
    prefix = ensure_prefix(prefix)
    return f"{prefix}/tpc/{test}/{name}".strip("/")


def dataset_paths_for_config(
    bucket: str, prefix: str, test: str, base_sf: float, repeats: int
) -> list[str]:
    """Returns *explicit path list* (not a glob) so we control exactly which objects are
    used.

    base_sf: the scale factor used for file naming (for stress tests, this is the per-file SF)
    """
    if test == "normal":
        key = s3_dataset_key(prefix, "normal", f"lineitem_{base_sf}.parquet")
        return [join_s3(bucket, key)]

    if test == "stress-small":
        paths = []
        for i in range(repeats):
            key = s3_dataset_key(
                prefix, "stress-small", f"lineitem_{base_sf}_{i:03d}.parquet"
            )
            paths.append(join_s3(bucket, key))
        return paths

    if test == "stress-big":
        paths = []
        for i in range(repeats):
            key = s3_dataset_key(
                prefix, "stress-big", f"lineitem_{base_sf}_{i:03d}.parquet"
            )
            paths.append(join_s3(bucket, key))
        return paths

    die(f"Unknown test: {test}")


def dataset_size_mb_for_config(
    bucket: str,
    prefix: str,
    test: str,
    base_sf: float,
    repeats: int,
    region: str | None,
) -> float:
    if test == "normal":
        key = s3_dataset_key(prefix, "normal", f"lineitem_{base_sf}.parquet")
        return s3_sum_size_mb(bucket, key, region)

    if test == "stress-small":
        base = s3_dataset_key(prefix, "stress-small", "")
        objs = s3_list_objects(bucket, base, region)
        wanted = {
            s3_dataset_key(
                prefix, "stress-small", f"lineitem_{base_sf}_{i:03d}.parquet"
            )
            for i in range(repeats)
        }
        total = sum(int(o["Size"]) for o in objs if o["Key"] in wanted)
        return total / (1024 * 1024)

    if test == "stress-big":
        base = s3_dataset_key(prefix, "stress-big", "")
        objs = s3_list_objects(bucket, base, region)
        wanted = {
            s3_dataset_key(prefix, "stress-big", f"lineitem_{base_sf}_{i:03d}.parquet")
            for i in range(repeats)
        }
        total = sum(int(o["Size"]) for o in objs if o["Key"] in wanted)
        return total / (1024 * 1024)

    die(f"Unknown test: {test}")


# -----------------------------
# Generate datasets on S3
# -----------------------------
def generate_normal(
    bucket: str,
    prefix: str,
    region: str | None,
    endpoint_url: str | None,
    row_group_size: int,
    threads: int | None,
    normal_sf: Sequence[int],
) -> None:
    first = True
    for sf in normal_sf:
        out_key = s3_dataset_key(prefix, "normal", f"lineitem_{sf}.parquet")
        out_path = join_s3(bucket, out_key)
        print(f"[generate normal] sf={sf} -> {out_path}", flush=True)

        if duckdb_backend() == "cli":
            sql_body = (
                f"CALL dbgen(sf={sf});\n"
                f"COPY lineitem TO '{out_path}' (FORMAT PARQUET, ROW_GROUP_SIZE {int(row_group_size)})"
            )
            script = _duckdb_cli_script(
                sql_body=sql_body,
                region=region,
                endpoint_url=endpoint_url,
                threads=threads,
                load_tpch=True,
            )
            _duckdb_cli_exec(sql_script=script, capture_stdout=False, csv_no_header=False)
            print(f"[generate normal] write complete", flush=True)
        else:
            con = duckdb_connect_for_s3(verbose=first)
            duckdb_set_threads(con, threads)
            duckdb_setup_s3(con, region, endpoint_url, verbose=first)
            duckdb_setup_tpch(con, verbose=first)

            print(f"[generate normal] calling dbgen(sf={sf})...", flush=True)
            con.execute(f"CALL dbgen(sf={sf});")
            print(f"[generate normal] dbgen complete, writing to {out_path}...", flush=True)
            # Export lineitem with explicit row group size
            con.execute(
                f"COPY lineitem TO '{out_path}' (FORMAT PARQUET, ROW_GROUP_SIZE {int(row_group_size)});"
            )
            print(f"[generate normal] write complete", flush=True)
            con.close()
        first = False


def ensure_copies(
    bucket: str,
    prefix: str,
    region: str | None,
    endpoint_url: str | None,
    test: str,
    base_sf: int,
    max_files: int,
    row_group_size: int,
    threads: int | None,
) -> None:
    """Create a base parquet and then server-side COPY it within S3 to create N files.

    This matches the stress test concept (many files of same size) without downloading
    locally.
    """
    base_name = f"lineitem_{base_sf}_000.parquet"
    base_key = s3_dataset_key(prefix, test, base_name)
    base_s3 = join_s3(bucket, base_key)

    # Create base file if missing
    existing = {
        o["Key"]
        for o in s3_list_objects(bucket, s3_dataset_key(prefix, test, ""), region)
    }
    if base_key not in existing:
        print(f"[generate {test}] creating base sf={base_sf} -> {base_s3}")
        if duckdb_backend() == "cli":
            sql_body = (
                f"CALL dbgen(sf={base_sf});\n"
                f"COPY lineitem TO '{base_s3}' (FORMAT PARQUET, ROW_GROUP_SIZE {int(row_group_size)})"
            )
            script = _duckdb_cli_script(
                sql_body=sql_body,
                region=region,
                endpoint_url=endpoint_url,
                threads=threads,
                load_tpch=True,
            )
            _duckdb_cli_exec(sql_script=script, capture_stdout=False, csv_no_header=False)
        else:
            con = duckdb_connect_for_s3()
            duckdb_set_threads(con, threads)
            duckdb_setup_s3(con, region, endpoint_url)
            duckdb_setup_tpch(con)
            con.execute(f"CALL dbgen(sf={base_sf});")
            con.execute(
                f"COPY lineitem TO '{base_s3}' (FORMAT PARQUET, ROW_GROUP_SIZE {int(row_group_size)});"
            )
            con.close()
        existing.add(base_key)

    # Create missing copies
    for i in range(max_files):
        key = s3_dataset_key(prefix, test, f"lineitem_{base_sf}_{i:03d}.parquet")
        if key in existing:
            continue
        print(f"[generate {test}] server-side copy {i:03d}: {base_key} -> {key}")
        s3_copy_object(bucket, base_key, key, region)


def generate_stress_sets(
    bucket: str,
    prefix: str,
    region: str | None,
    endpoint_url: str | None,
    row_group_size: int,
    threads: int | None,
    stress_small_base_sf: float,
    stress_small_repeats: Sequence[int],
    stress_big_base_sf: float,
    stress_big_repeats: Sequence[int],
) -> None:
    if stress_small_repeats:
        ensure_copies(
            bucket=bucket,
            prefix=prefix,
            region=region,
            endpoint_url=endpoint_url,
            test="stress-small",
            base_sf=stress_small_base_sf,
            max_files=max(stress_small_repeats),
            row_group_size=row_group_size,
            threads=threads,
        )
    if stress_big_repeats:
        ensure_copies(
            bucket=bucket,
            prefix=prefix,
            region=region,
            endpoint_url=endpoint_url,
            test="stress-big",
            base_sf=stress_big_base_sf,
            max_files=max(stress_big_repeats),
            row_group_size=row_group_size,
            threads=threads,
        )


# -----------------------------
# String table generation (for join/group-by benchmarks)
# -----------------------------
def string_table_path(bucket: str, prefix: str, table: str, scale_k: int) -> str:
    """Get S3 path for a string table at given scale (thousands of rows)."""
    key = f"{ensure_prefix(prefix)}/string_tables/{table}_{scale_k}k.parquet"
    return join_s3(bucket, key)


def generate_string_tables(
    bucket: str,
    prefix: str,
    region: str | None,
    endpoint_url: str | None,
    row_group_size: int,
    threads: int | None,
    scales: Sequence[int],
) -> None:
    """Generate string tables for join/group-by benchmarks.

    Creates table_a and table_b with:
    - 7 VARCHAR columns each (widths 4-17)
    - 3 primary key columns (pk1, pk2, pk3)
    - Sorted by primary key
    - table_b has same PKs as table_a for joins
    """
    use_cli = duckdb_backend() == "cli"
    con: duckdb.DuckDBPyConnection | None = None
    if not use_cli:
        con = duckdb_connect_for_s3()
        duckdb_set_threads(con, threads)
        duckdb_setup_s3(con, region, endpoint_url)

    for scale_k in scales:
        num_rows = scale_k * 1000
        num_groups = max(1, num_rows // GROUPBY_CARDINALITY)

        path_a = string_table_path(bucket, prefix, "table_a", scale_k)
        path_b = string_table_path(bucket, prefix, "table_b", scale_k)

        print(f"[generate string] {scale_k}K rows ({num_groups:,} groups) -> {path_a}")

        gen_a_sql = f"""
            COPY (
                SELECT
                    printf('%04d', (row_number() OVER ()) % {num_groups}) as pk1,
                    printf('%08d', (row_number() OVER ()) // {num_groups}) as pk2,
                    printf('%010d', row_number() OVER ()) as pk3,
                    substr(md5(random()::text), 1, 6) as str1,
                    substr(md5(random()::text), 1, 12) as str2,
                    substr(md5(random()::text), 1, 15) as str3,
                    substr(md5(random()::text) || md5(random()::text), 1, 17) as str4
                FROM generate_series(1, {num_rows})
                ORDER BY pk1, pk2, pk3
            ) TO '{path_a}' (FORMAT PARQUET, ROW_GROUP_SIZE {int(row_group_size)})
        """.strip()

        if use_cli:
            script = _duckdb_cli_script(
                sql_body=gen_a_sql,
                region=region,
                endpoint_url=endpoint_url,
                threads=threads,
                load_tpch=False,
            )
            _duckdb_cli_exec(sql_script=script, capture_stdout=False, csv_no_header=False)
        else:
            assert con is not None
            con.execute(gen_a_sql + ";")

        print(f"[generate string] {scale_k}K rows -> {path_b}")

        gen_b_sql = f"""
            COPY (
                SELECT
                    pk1, pk2, pk3,
                    substr(md5(random()::text), 1, 6) as str1,
                    substr(md5(random()::text), 1, 12) as str2,
                    substr(md5(random()::text), 1, 15) as str3,
                    substr(md5(random()::text) || md5(random()::text), 1, 17) as str4
                FROM read_parquet('{path_a}')
                ORDER BY pk1, pk2, pk3
            ) TO '{path_b}' (FORMAT PARQUET, ROW_GROUP_SIZE {int(row_group_size)})
        """.strip()

        if use_cli:
            script = _duckdb_cli_script(
                sql_body=gen_b_sql,
                region=region,
                endpoint_url=endpoint_url,
                threads=threads,
                load_tpch=False,
            )
            _duckdb_cli_exec(sql_script=script, capture_stdout=False, csv_no_header=False)
        else:
            assert con is not None
            con.execute(gen_b_sql + ";")

    if con is not None:
        con.close()


# -----------------------------
# Join and Group-by benchmark functions
# -----------------------------
def duckdb_run_join_python(
    path_a: str,
    path_b: str,
    out_s3_prefix: str,
    row_group_size: int,
    threads: int | None,
    region: str | None,
    endpoint_url: str | None,
) -> dict[str, Any]:
    """Run join benchmark with DuckDB."""
    con = duckdb_connect_for_s3()
    duckdb_set_threads(con, threads)
    duckdb_setup_s3(con, region, endpoint_url)

    proc = psutil.Process(os.getpid())
    baseline_rss = proc.memory_info().rss
    peak_rss = baseline_rss
    stop = threading.Event()

    def sampler():
        nonlocal peak_rss
        while not stop.is_set():
            try:
                rss = proc.memory_info().rss
                if rss > peak_rss:
                    peak_rss = rss
            except Exception:
                pass
            time.sleep(DEFAULT_SAMPLE_INTERVAL_S)

    th = threading.Thread(target=sampler, daemon=True)
    th.start()

    run_id = str(uuid.uuid4())
    written_prefix = out_s3_prefix.rstrip("/") + f"/duckdb/join/{run_id}"

    t0 = time.perf_counter()
    con.execute(f"""
        COPY (
            SELECT
                a.pk1, a.pk2, a.pk3,
                a.str1 as a_str1, a.str2 as a_str2, a.str3 as a_str3, a.str4 as a_str4,
                b.str1 as b_str1, b.str2 as b_str2, b.str3 as b_str3, b.str4 as b_str4
            FROM read_parquet('{path_a}') a
            INNER JOIN read_parquet('{path_b}') b
                ON a.pk1 = b.pk1 AND a.pk2 = b.pk2 AND a.pk3 = b.pk3
        ) TO '{written_prefix}' (FORMAT PARQUET, PER_THREAD_OUTPUT, ROW_GROUP_SIZE {row_group_size});
    """)
    t1 = time.perf_counter()

    stop.set()
    th.join(timeout=1.0)
    con.close()

    return {
        "time_s": t1 - t0,
        "memory_mb": (peak_rss - baseline_rss) / (1024 * 1024),
        "written_prefix": written_prefix,
    }


def duckdb_run_join_cli(
    path_a: str,
    path_b: str,
    out_s3_prefix: str,
    row_group_size: int,
    threads: int | None,
    region: str | None,
    endpoint_url: str | None,
) -> dict[str, Any]:
    run_id = str(uuid.uuid4())
    written_prefix = out_s3_prefix.rstrip("/") + f"/duckdb/join/{run_id}"
    sql_body = f"""
        COPY (
            SELECT
                a.pk1, a.pk2, a.pk3,
                a.str1 as a_str1, a.str2 as a_str2, a.str3 as a_str3, a.str4 as a_str4,
                b.str1 as b_str1, b.str2 as b_str2, b.str3 as b_str3, b.str4 as b_str4
            FROM read_parquet('{path_a}') a
            INNER JOIN read_parquet('{path_b}') b
                ON a.pk1 = b.pk1 AND a.pk2 = b.pk2 AND a.pk3 = b.pk3
        ) TO '{written_prefix}' (FORMAT PARQUET, PER_THREAD_OUTPUT, ROW_GROUP_SIZE {row_group_size})
    """.strip()
    script = _duckdb_cli_script(
        sql_body=sql_body,
        region=region,
        endpoint_url=endpoint_url,
        threads=threads,
        load_tpch=False,
    )
    time_s, mem_mb, _ = _duckdb_cli_exec(
        sql_script=script, capture_stdout=False, csv_no_header=False
    )
    return {"time_s": time_s, "memory_mb": mem_mb, "written_prefix": written_prefix}


def duckdb_run_join(
    path_a: str,
    path_b: str,
    out_s3_prefix: str,
    row_group_size: int,
    threads: int | None,
    region: str | None,
    endpoint_url: str | None,
) -> dict[str, Any]:
    if duckdb_backend() == "cli":
        return duckdb_run_join_cli(
            path_a=path_a,
            path_b=path_b,
            out_s3_prefix=out_s3_prefix,
            row_group_size=row_group_size,
            threads=threads,
            region=region,
            endpoint_url=endpoint_url,
        )
    return duckdb_run_join_python(
        path_a=path_a,
        path_b=path_b,
        out_s3_prefix=out_s3_prefix,
        row_group_size=row_group_size,
        threads=threads,
        region=region,
        endpoint_url=endpoint_url,
    )


def polars_run_join(
    path_a: str,
    path_b: str,
    out_s3_prefix: str,
    row_group_size: int,
    threads: int | None,
    storage_options: dict[str, str] | None,
) -> dict[str, Any]:
    """Run join benchmark with Polars."""
    polars_set_threads(threads)

    lf_a = pl.scan_parquet(path_a, storage_options=storage_options)
    lf_b = pl.scan_parquet(path_b, storage_options=storage_options)

    proc = psutil.Process(os.getpid())
    baseline_rss = proc.memory_info().rss
    peak_rss = baseline_rss
    stop = threading.Event()

    def sampler():
        nonlocal peak_rss
        while not stop.is_set():
            try:
                rss = proc.memory_info().rss
                if rss > peak_rss:
                    peak_rss = rss
            except Exception:
                pass
            time.sleep(DEFAULT_SAMPLE_INTERVAL_S)

    th = threading.Thread(target=sampler, daemon=True)
    th.start()

    run_id = str(uuid.uuid4())
    out_path = out_s3_prefix.rstrip("/") + f"/polars/join/{run_id}.parquet"

    t0 = time.perf_counter()
    lf_a.join(lf_b, on=["pk1", "pk2", "pk3"], how="inner", suffix="_b").sink_parquet(
        out_path, storage_options=storage_options, row_group_size=row_group_size
    )
    t1 = time.perf_counter()

    stop.set()
    th.join(timeout=1.0)

    return {
        "time_s": t1 - t0,
        "memory_mb": (peak_rss - baseline_rss) / (1024 * 1024),
        "written_prefix": out_path,
    }


def duckdb_run_groupby_python(
    path_a: str,
    out_s3_prefix: str,
    row_group_size: int,
    threads: int | None,
    region: str | None,
    endpoint_url: str | None,
) -> dict[str, Any]:
    """Run group-by benchmark with DuckDB."""
    con = duckdb_connect_for_s3()
    duckdb_set_threads(con, threads)
    duckdb_setup_s3(con, region, endpoint_url)

    proc = psutil.Process(os.getpid())
    baseline_rss = proc.memory_info().rss
    peak_rss = baseline_rss
    stop = threading.Event()

    def sampler():
        nonlocal peak_rss
        while not stop.is_set():
            try:
                rss = proc.memory_info().rss
                if rss > peak_rss:
                    peak_rss = rss
            except Exception:
                pass
            time.sleep(DEFAULT_SAMPLE_INTERVAL_S)

    th = threading.Thread(target=sampler, daemon=True)
    th.start()

    run_id = str(uuid.uuid4())
    written_prefix = out_s3_prefix.rstrip("/") + f"/duckdb/groupby/{run_id}"

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
            FROM read_parquet('{path_a}')
            GROUP BY pk1
        ) TO '{written_prefix}' (FORMAT PARQUET, PER_THREAD_OUTPUT, ROW_GROUP_SIZE {row_group_size});
    """)
    t1 = time.perf_counter()

    stop.set()
    th.join(timeout=1.0)
    con.close()

    return {
        "time_s": t1 - t0,
        "memory_mb": (peak_rss - baseline_rss) / (1024 * 1024),
        "written_prefix": written_prefix,
    }


def duckdb_run_groupby_cli(
    path_a: str,
    out_s3_prefix: str,
    row_group_size: int,
    threads: int | None,
    region: str | None,
    endpoint_url: str | None,
) -> dict[str, Any]:
    run_id = str(uuid.uuid4())
    written_prefix = out_s3_prefix.rstrip("/") + f"/duckdb/groupby/{run_id}"
    sql_body = f"""
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
            FROM read_parquet('{path_a}')
            GROUP BY pk1
        ) TO '{written_prefix}' (FORMAT PARQUET, PER_THREAD_OUTPUT, ROW_GROUP_SIZE {row_group_size})
    """.strip()
    script = _duckdb_cli_script(
        sql_body=sql_body,
        region=region,
        endpoint_url=endpoint_url,
        threads=threads,
        load_tpch=False,
    )
    time_s, mem_mb, _ = _duckdb_cli_exec(
        sql_script=script, capture_stdout=False, csv_no_header=False
    )
    return {"time_s": time_s, "memory_mb": mem_mb, "written_prefix": written_prefix}


def duckdb_run_groupby(
    path_a: str,
    out_s3_prefix: str,
    row_group_size: int,
    threads: int | None,
    region: str | None,
    endpoint_url: str | None,
) -> dict[str, Any]:
    if duckdb_backend() == "cli":
        return duckdb_run_groupby_cli(
            path_a=path_a,
            out_s3_prefix=out_s3_prefix,
            row_group_size=row_group_size,
            threads=threads,
            region=region,
            endpoint_url=endpoint_url,
        )
    return duckdb_run_groupby_python(
        path_a=path_a,
        out_s3_prefix=out_s3_prefix,
        row_group_size=row_group_size,
        threads=threads,
        region=region,
        endpoint_url=endpoint_url,
    )


def polars_run_groupby(
    path_a: str,
    out_s3_prefix: str,
    row_group_size: int,
    threads: int | None,
    storage_options: dict[str, str] | None,
) -> dict[str, Any]:
    """Run group-by benchmark with Polars."""
    polars_set_threads(threads)

    lf = pl.scan_parquet(path_a, storage_options=storage_options)

    # Build aggregations
    agg_cols = ["pk2", "pk3", "str1", "str2", "str3", "str4"]
    aggs = []
    for col in agg_cols:
        aggs.append(pl.col(col).min().alias(f"min_{col}"))
        aggs.append(pl.col(col).max().alias(f"max_{col}"))
    aggs.append(pl.len().alias("row_count"))

    proc = psutil.Process(os.getpid())
    baseline_rss = proc.memory_info().rss
    peak_rss = baseline_rss
    stop = threading.Event()

    def sampler():
        nonlocal peak_rss
        while not stop.is_set():
            try:
                rss = proc.memory_info().rss
                if rss > peak_rss:
                    peak_rss = rss
            except Exception:
                pass
            time.sleep(DEFAULT_SAMPLE_INTERVAL_S)

    th = threading.Thread(target=sampler, daemon=True)
    th.start()

    run_id = str(uuid.uuid4())
    out_path = out_s3_prefix.rstrip("/") + f"/polars/groupby/{run_id}.parquet"

    t0 = time.perf_counter()
    lf.group_by("pk1").agg(aggs).sink_parquet(
        out_path, storage_options=storage_options, row_group_size=row_group_size
    )
    t1 = time.perf_counter()

    stop.set()
    th.join(timeout=1.0)

    return {
        "time_s": t1 - t0,
        "memory_mb": (peak_rss - baseline_rss) / (1024 * 1024),
        "written_prefix": out_path,
    }


def run_string_benchmarks(
    bucket: str,
    prefix: str,
    region: str | None,
    endpoint_url: str | None,
    tools: list[str],
    modes: list[str],
    iterations: int,
    out_dir: Path,
    out_s3_prefix: str,
    cleanup_s3_output: bool,
    row_group_size: int,
    threads: int | None,
    scales: list[int] | None = None,
) -> None:
    """Run join and/or group-by benchmarks on string tables."""
    out_dir.mkdir(parents=True, exist_ok=True)
    scales = scales if scales is not None else get_string_scales()
    storage_options = polars_storage_options_from_env(region, endpoint_url)

    for mode in modes:
        rows: list[dict[str, Any]] = []

        for scale_k in scales:
            path_a = string_table_path(bucket, prefix, "table_a", scale_k)
            path_b = string_table_path(bucket, prefix, "table_b", scale_k)

            for tool in tools:
                for it in range(iterations):
                    if mode == "join":
                        if tool == "duckdb":
                            res = duckdb_run_join(
                                path_a,
                                path_b,
                                out_s3_prefix,
                                row_group_size,
                                threads,
                                region,
                                endpoint_url,
                            )
                        else:
                            res = polars_run_join(
                                path_a,
                                path_b,
                                out_s3_prefix,
                                row_group_size,
                                threads,
                                storage_options,
                            )
                    elif mode == "groupby":
                        if tool == "duckdb":
                            res = duckdb_run_groupby(
                                path_a,
                                out_s3_prefix,
                                row_group_size,
                                threads,
                                region,
                                endpoint_url,
                            )
                        else:
                            res = polars_run_groupby(
                                path_a,
                                out_s3_prefix,
                                row_group_size,
                                threads,
                                storage_options,
                            )
                    else:
                        die(f"Unknown string mode: {mode}")

                    written_prefix = res.get("written_prefix")
                    written_mb = None
                    if written_prefix:
                        wb, wk = parse_s3_url(written_prefix)
                        written_mb = s3_sum_size_mb(wb, wk, region)
                        if cleanup_s3_output:
                            s3_delete_prefix(wb, wk, region)

                    rows.append(
                        {
                            "tool": tool,
                            "mode": mode,
                            "scale_k": scale_k,
                            "rows": scale_k * 1000,
                            "time_s": res["time_s"],
                            "memory_mb": res["memory_mb"],
                            "written_mb": written_mb,
                            "iteration": it,
                        }
                    )
                    print(
                        f"[{tool:6s} {mode:7s} {scale_k}K it={it:02d}] time={res['time_s']:.3f}s mem={res['memory_mb']:.1f}MB"
                    )

        # Save results
        df = pd.DataFrame(rows)
        df.to_csv(out_dir / f"results_raw_{mode}.csv", index=False)

        for tool in tools:
            sub = df[df["tool"] == tool].copy()
            if sub.empty:
                continue
            grp = (
                sub.groupby(["tool", "mode", "scale_k", "rows"], as_index=False)
                .agg(
                    time_s=("time_s", "mean"),
                    memory_mb=("memory_mb", "mean"),
                    written_mb=("written_mb", "mean"),
                )
                .sort_values("scale_k")
            )
            grp.to_csv(out_dir / f"{tool}_{mode}.csv", index=False)

    print(f"\nWrote CSVs to: {out_dir}")


# -----------------------------
# Plotting for string benchmarks
# -----------------------------
def plot_string_benchmark(out_dir: Path, mode: str) -> bool:
    """Plot join or group-by benchmark results."""
    duck = out_dir / f"duckdb_{mode}.csv"
    pol = out_dir / f"polars_{mode}.csv"
    if not duck.exists() or not pol.exists():
        print(f"Skipping {mode}: missing CSVs")
        return False

    ddf = pd.read_csv(duck)
    pdf = pd.read_csv(pol)
    merged = pd.concat([ddf, pdf], ignore_index=True).sort_values(["rows", "tool"])

    # Time plot
    plt.figure(figsize=(10, 6))
    for tool in ["duckdb", "polars"]:
        sub = merged[merged["tool"] == tool].sort_values("rows")
        plt.plot(sub["rows"], sub["time_s"], marker="o", label=tool)
    plt.xlabel("Number of Rows")
    plt.ylabel("Time (s)")
    plt.title(f"{mode} benchmark - time")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_dir / f"polars_duckdb_{mode}_time.png")
    plt.close()

    # Memory plot
    plt.figure(figsize=(10, 6))
    for tool in ["duckdb", "polars"]:
        sub = merged[merged["tool"] == tool].sort_values("rows")
        plt.plot(sub["rows"], sub["memory_mb"], marker="o", label=tool)
    plt.xlabel("Number of Rows")
    plt.ylabel("Peak RSS delta (MB)")
    plt.title(f"{mode} benchmark - memory")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_dir / f"polars_duckdb_{mode}_memory.png")
    plt.close()

    return True


# -----------------------------
# Benchmark runner + CSV
# -----------------------------
def configs_for_test(
    test: str,
    normal_sf: Sequence[float],
    stress_small_base_sf: float,
    stress_small_repeats: Sequence[int],
    stress_big_base_sf: float,
    stress_big_repeats: Sequence[int],
) -> list[tuple[float, float, int]]:
    """Returns list of (effective_scale_factor, base_sf, repeats)

    - normal: base_sf=effective_sf, repeats=1
    - stress-small: base_sf from config, effective_sf=base_sf*repeats
    - stress-big: base_sf from config, effective_sf=base_sf*repeats
    """
    if test == "normal":
        return [(sf, sf, 1) for sf in normal_sf]
    if test == "stress-small":
        return [
            (stress_small_base_sf * r, stress_small_base_sf, r)
            for r in stress_small_repeats
        ]
    if test == "stress-big":
        return [
            (stress_big_base_sf * r, stress_big_base_sf, r) for r in stress_big_repeats
        ]
    die(f"Unknown test: {test}")


def run_benchmarks(
    bucket: str,
    prefix: str,
    region: str | None,
    endpoint_url: str | None,
    tools: list[str],
    tests: list[str],
    mode: str,
    iterations: int,
    out_dir: Path,
    out_s3_prefix: str | None,
    cleanup_s3_output: bool,
    row_group_size: int,
    threads: int | None,
    polars_streaming: bool,
    polars_sink_engine: str,
    normal_sf: list[float] | None = None,
    stress_small_repeats: list[int] | None = None,
    stress_big_repeats: list[int] | None = None,
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, Any]] = []

    # Resolve scale factors from CLI args or env vars
    normal_sf_values = _resolve_sequence(normal_sf, get_normal_sf)
    stress_small_base = get_stress_small_base_sf()
    stress_small_values = _resolve_sequence(
        stress_small_repeats, get_stress_small_repeats
    )
    stress_big_base = get_stress_big_base_sf()
    stress_big_values = _resolve_sequence(stress_big_repeats, get_stress_big_repeats)

    # Build Polars storage_options from env if explicit env creds are provided
    storage_options = polars_storage_options_from_env(region, endpoint_url)

    for test in tests:
        for eff_sf, base_sf, repeats in configs_for_test(
            test,
            normal_sf=normal_sf_values,
            stress_small_base_sf=stress_small_base,
            stress_small_repeats=stress_small_values,
            stress_big_base_sf=stress_big_base,
            stress_big_repeats=stress_big_values,
        ):
            paths = dataset_paths_for_config(bucket, prefix, test, base_sf, repeats)
            size_mb = dataset_size_mb_for_config(
                bucket, prefix, test, base_sf, repeats, region
            )

            for tool in tools:
                for it in range(iterations):
                    payload = {
                        "tool": tool,
                        "paths": paths,
                        "mode": mode,
                        "out_s3_prefix": out_s3_prefix,
                        "row_group_size": row_group_size,
                        "threads": threads,
                        "region": region,
                        "endpoint_url": endpoint_url,
                        "storage_options": storage_options,
                        "streaming": polars_streaming,
                        "sink_engine": polars_sink_engine,
                    }
                    res = run_in_fresh_process(payload)

                    written_prefix = res.get("written_prefix")
                    written_mb = None
                    if written_prefix:
                        wb, wk = parse_s3_url(written_prefix)
                        # if it's a file path, prefix is the key; if it's a "folder", we list by prefix
                        written_mb = s3_sum_size_mb(wb, wk, region)

                        if cleanup_s3_output:
                            # delete outside timed section
                            s3_delete_prefix(wb, wk, region)

                    rows.append(
                        {
                            "tool": tool,
                            "test": test,
                            "mode": mode,
                            "scale_factor": eff_sf,
                            "base_sf": base_sf,
                            "repeats": repeats,
                            "dataset_size_mb": size_mb,
                            "time_s": res["time_s"],
                            "memory_mb": res["memory_mb"],
                            "row_count": res.get("row_count"),
                            "written_mb": written_mb,
                            "iteration": it,
                        }
                    )
                    print(
                        f"[{tool:6s} {test:11s} sf={eff_sf:6.1f} it={it:02d}] "
                        f"time={res['time_s']:.3f}s mem={res['memory_mb']:.1f}MB "
                        + (
                            f"written={written_mb:.1f}MB"
                            if written_mb is not None
                            else ""
                        )
                    )

    df = pd.DataFrame(rows)
    # Raw results
    raw_path = out_dir / f"results_raw_{mode}.csv"
    df.to_csv(raw_path, index=False)

    # Per-tool/per-test summaries (mean), like the repo
    for tool in tools:
        for test in tests:
            sub = df[(df["tool"] == tool) & (df["test"] == test)].copy()
            if sub.empty:
                continue
            out_csv = (
                out_dir / f"{tool}_{test}{'' if mode == 'count' else '_' + mode}.csv"
            )
            grp = (
                sub.groupby(
                    ["tool", "test", "mode", "scale_factor", "dataset_size_mb"],
                    as_index=False,
                )
                .agg(
                    time_s=("time_s", "mean"),
                    memory_mb=("memory_mb", "mean"),
                    row_count=("row_count", "first"),
                    written_mb=("written_mb", "mean"),
                )
                .sort_values("scale_factor")
            )
            grp.to_csv(out_csv, index=False)

    print(f"\nWrote CSVs to: {out_dir}")


# -----------------------------
# Plotting (matplotlib; same plot set as post/repo)
# -----------------------------
def plot_test(out_dir: Path, test: str, mode: str) -> bool:
    """Plot a single test.

    Returns True if successful, False if CSVs are missing.
    """
    duck = out_dir / f"duckdb_{test}{'' if mode == 'count' else '_' + mode}.csv"
    pol = out_dir / f"polars_{test}{'' if mode == 'count' else '_' + mode}.csv"
    if not duck.exists() or not pol.exists():
        print(f"Skipping {test}: missing CSVs")
        return False

    ddf = pd.read_csv(duck)
    pdf = pd.read_csv(pol)

    merged = pd.concat([ddf, pdf], ignore_index=True).sort_values(
        ["scale_factor", "tool"]
    )

    # Time plot
    plt.figure()
    for tool in ["duckdb", "polars"]:
        sub = merged[merged["tool"] == tool].sort_values("scale_factor")
        plt.plot(sub["dataset_size_mb"], sub["time_s"], marker="o", label=tool)
    plt.xlabel("Dataset size (MB)")
    plt.ylabel("Time (s)")
    plt.title(f"{test} - time ({mode})")
    plt.legend()
    plt.tight_layout()
    plt.savefig(
        out_dir
        / f"polars_duckdb_{test}_time{'' if mode == 'count' else '_' + mode}.png"
    )
    plt.close()

    # Memory plot
    plt.figure()
    for tool in ["duckdb", "polars"]:
        sub = merged[merged["tool"] == tool].sort_values("scale_factor")
        plt.plot(sub["dataset_size_mb"], sub["memory_mb"], marker="o", label=tool)
    plt.xlabel("Dataset size (MB)")
    plt.ylabel("Peak RSS delta (MB)")
    plt.title(f"{test} - memory ({mode})")
    plt.legend()
    plt.tight_layout()
    plt.savefig(
        out_dir
        / f"polars_duckdb_{test}_memory{'' if mode == 'count' else '_' + mode}.png"
    )
    plt.close()
    return True


def plot_overlay(out_dir: Path, mode: str) -> bool:
    """Overlay normal vs stress-small (like the post/repo overlay plots).

    Returns True if successful, False if required CSVs are missing.
    """
    suffix = "" if mode == "count" else "_" + mode
    required_files = [
        out_dir / f"duckdb_normal{suffix}.csv",
        out_dir / f"polars_normal{suffix}.csv",
        out_dir / f"duckdb_stress-small{suffix}.csv",
        out_dir / f"polars_stress-small{suffix}.csv",
    ]
    missing = [f for f in required_files if not f.exists()]
    if missing:
        print(f"Skipping overlay plot: missing {len(missing)} CSV(s)")
        return False

    normal_duck = pd.read_csv(out_dir / f"duckdb_normal{suffix}.csv")
    normal_pol = pd.read_csv(out_dir / f"polars_normal{suffix}.csv")
    small_duck = pd.read_csv(out_dir / f"duckdb_stress-small{suffix}.csv")
    small_pol = pd.read_csv(out_dir / f"polars_stress-small{suffix}.csv")

    def prep(df: pd.DataFrame, label: str) -> pd.DataFrame:
        x = df.copy()
        x["layout"] = label
        return x

    overlay = pd.concat(
        [
            prep(normal_duck, "normal"),
            prep(normal_pol, "normal"),
            prep(small_duck, "stress-small"),
            prep(small_pol, "stress-small"),
        ],
        ignore_index=True,
    )

    # Overlay time
    plt.figure()
    for (tool, layout), sub in overlay.groupby(["tool", "layout"]):
        sub = sub.sort_values("dataset_size_mb")
        plt.plot(
            sub["dataset_size_mb"], sub["time_s"], marker="o", label=f"{tool}-{layout}"
        )
    plt.xlabel("Dataset size (MB)")
    plt.ylabel("Time (s)")
    plt.title(f"overlay - time ({mode})")
    plt.legend()
    plt.tight_layout()
    plt.savefig(
        out_dir
        / f"polars_duckdb_overlay_time{'' if mode == 'count' else '_' + mode}.png"
    )
    plt.close()

    # Overlay memory
    plt.figure()
    for (tool, layout), sub in overlay.groupby(["tool", "layout"]):
        sub = sub.sort_values("dataset_size_mb")
        plt.plot(
            sub["dataset_size_mb"],
            sub["memory_mb"],
            marker="o",
            label=f"{tool}-{layout}",
        )
    plt.xlabel("Dataset size (MB)")
    plt.ylabel("Peak RSS delta (MB)")
    plt.title(f"overlay - memory ({mode})")
    plt.legend()
    plt.tight_layout()
    plt.savefig(
        out_dir
        / f"polars_duckdb_overlay_memory{'' if mode == 'count' else '_' + mode}.png"
    )
    plt.close()
    return True


# -----------------------------
# CLI
# -----------------------------
def main() -> None:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)

    # generate
    apg = sub.add_parser(
        "generate",
        help="Generate TPC-H lineitem Parquet datasets to S3 (DuckDB tpch/dbgen).",
    )
    apg.add_argument("--bucket", required=True)
    apg.add_argument(
        "--prefix",
        required=True,
        help="S3 key prefix (folder) to store datasets under.",
    )
    apg.add_argument("--region", default=None)
    apg.add_argument("--row-group-size", type=int, default=DEFAULT_ROW_GROUP_SIZE)
    apg.add_argument("--threads", type=int, default=None)
    apg.add_argument(
        "--normal-only",
        action="store_true",
        help="Only generate normal single-file datasets.",
    )
    apg.add_argument(
        "--stress-only",
        action="store_true",
        help="Only generate stress datasets (many files).",
    )
    apg.add_argument(
        "--normal-sf",
        type=int,
        nargs="+",
        default=None,
        help="Override normal scale factors (default: internal NORMAL_SF list).",
    )
    apg.add_argument(
        "--stress-small-repeats",
        type=int,
        nargs="+",
        default=None,
        help="Override stress-small repeats (default: internal STRESS_SMALL_REPEATS list).",
    )
    apg.add_argument(
        "--stress-big-repeats",
        type=int,
        nargs="+",
        default=None,
        help="Override stress-big repeats (default: internal STRESS_BIG_REPEATS list).",
    )

    # run
    apr = sub.add_parser(
        "run", help="Run benchmarks from S3 datasets and write CSV results."
    )
    apr.add_argument("--bucket", required=True)
    apr.add_argument("--prefix", required=True)
    apr.add_argument("--region", default=None)
    apr.add_argument("--tool", choices=["duckdb", "polars", "all"], default="all")
    apr.add_argument(
        "--test", choices=["normal", "stress-small", "stress-big", "all"], default="all"
    )
    apr.add_argument(
        "--mode", choices=["count", "filter_write", "copy_project"], default="count"
    )
    apr.add_argument("--iterations", type=int, default=DEFAULT_ITERATIONS)
    apr.add_argument(
        "--out-dir", default="results_s3", help="Local directory for CSV+PNG outputs."
    )
    apr.add_argument(
        "--out-s3-prefix",
        default=None,
        help="S3 prefix to write benchmark outputs for write modes.",
    )
    apr.add_argument(
        "--cleanup-s3-output",
        action="store_true",
        help="Delete written outputs after each run (outside timing).",
    )
    apr.add_argument("--row-group-size", type=int, default=DEFAULT_ROW_GROUP_SIZE)
    apr.add_argument("--threads", type=int, default=None)
    apr.add_argument(
        "--polars-streaming",
        action="store_true",
        help="Use Polars collect(streaming=True) in count mode.",
    )
    apr.add_argument(
        "--polars-sink-engine",
        default="auto",
        help="Polars sink_parquet engine (auto/pyarrow/...).",
    )
    apr.add_argument(
        "--normal-sf",
        type=int,
        nargs="+",
        default=None,
        help="Override normal scale factors to benchmark (default: internal NORMAL_SF list).",
    )
    apr.add_argument(
        "--stress-small-repeats",
        type=int,
        nargs="+",
        default=None,
        help="Override stress-small repeats to benchmark (default: internal STRESS_SMALL_REPEATS list).",
    )
    apr.add_argument(
        "--stress-big-repeats",
        type=int,
        nargs="+",
        default=None,
        help="Override stress-big repeats to benchmark (default: internal STRESS_BIG_REPEATS list).",
    )

    # plot
    app = sub.add_parser(
        "plot", help="Generate plots from CSVs (time/memory + overlay)."
    )
    app.add_argument("--out-dir", default="results_s3")
    app.add_argument(
        "--mode", choices=["count", "filter_write", "copy_project"], default="count"
    )
    app.add_argument(
        "--region",
        default=None,
        help="Optional AWS region for S3 access (defaults to env chain).",
    )

    # generate-string
    apgs = sub.add_parser(
        "generate-string", help="Generate string tables for join/group-by benchmarks."
    )
    apgs.add_argument("--bucket", required=True)
    apgs.add_argument("--prefix", required=True)
    apgs.add_argument("--region", default=None)
    apgs.add_argument("--row-group-size", type=int, default=DEFAULT_ROW_GROUP_SIZE)
    apgs.add_argument("--threads", type=int, default=None)
    apgs.add_argument(
        "--scales",
        type=int,
        nargs="+",
        default=None,
        help="Row counts in thousands (default: env or 10 50 100 500 1000)",
    )

    # run-string
    aprs = sub.add_parser(
        "run-string", help="Run join/group-by benchmarks on string tables."
    )
    aprs.add_argument("--bucket", required=True)
    aprs.add_argument("--prefix", required=True)
    aprs.add_argument("--region", default=None)
    aprs.add_argument("--tool", choices=["duckdb", "polars", "all"], default="all")
    aprs.add_argument("--mode", choices=["join", "groupby", "all"], default="all")
    aprs.add_argument("--iterations", type=int, default=DEFAULT_ITERATIONS)
    aprs.add_argument(
        "--out-dir",
        default="results_string",
        help="Local directory for CSV+PNG outputs.",
    )
    aprs.add_argument(
        "--out-s3-prefix", required=True, help="S3 prefix to write benchmark outputs."
    )
    aprs.add_argument("--cleanup-s3-output", action="store_true")
    aprs.add_argument("--row-group-size", type=int, default=DEFAULT_ROW_GROUP_SIZE)
    aprs.add_argument("--threads", type=int, default=None)
    aprs.add_argument(
        "--scales", type=int, nargs="+", default=None, help="Row counts in thousands"
    )

    # plot-string
    apps = sub.add_parser(
        "plot-string", help="Generate plots for join/group-by benchmarks."
    )
    apps.add_argument("--out-dir", default="results_string")

    args = ap.parse_args()
    region = aws_region(getattr(args, "region", None))
    endpoint_url = aws_endpoint_url()

    if args.cmd == "generate":
        if args.normal_only and args.stress_only:
            die("Choose at most one of --normal-only or --stress-only")
        ensure_s3_bucket_exists(args.bucket, region)
        normal_sf = _resolve_sequence(args.normal_sf, get_normal_sf)
        stress_small_base = get_stress_small_base_sf()
        stress_small_repeats = _resolve_sequence(
            args.stress_small_repeats, get_stress_small_repeats
        )
        stress_big_base = get_stress_big_base_sf()
        stress_big_repeats = _resolve_sequence(
            args.stress_big_repeats, get_stress_big_repeats
        )
        if not args.stress_only:
            generate_normal(
                bucket=args.bucket,
                prefix=args.prefix,
                region=region,
                endpoint_url=endpoint_url,
                row_group_size=args.row_group_size,
                threads=args.threads,
                normal_sf=normal_sf,
            )
        if not args.normal_only:
            generate_stress_sets(
                bucket=args.bucket,
                prefix=args.prefix,
                region=region,
                endpoint_url=endpoint_url,
                row_group_size=args.row_group_size,
                threads=args.threads,
                stress_small_base_sf=stress_small_base,
                stress_small_repeats=stress_small_repeats,
                stress_big_base_sf=stress_big_base,
                stress_big_repeats=stress_big_repeats,
            )
        print("Done.")

    elif args.cmd == "run":
        tools = ["duckdb", "polars"] if args.tool == "all" else [args.tool]
        tests = (
            ["normal", "stress-small", "stress-big"]
            if args.test == "all"
            else [args.test]
        )
        out_dir = Path(args.out_dir)

        if args.mode != "count" and not args.out_s3_prefix:
            die("--out-s3-prefix is required for filter_write or copy_project modes")

        run_benchmarks(
            bucket=args.bucket,
            prefix=args.prefix,
            region=region,
            endpoint_url=endpoint_url,
            tools=tools,
            tests=tests,
            mode=args.mode,
            iterations=args.iterations,
            out_dir=out_dir,
            out_s3_prefix=args.out_s3_prefix,
            cleanup_s3_output=args.cleanup_s3_output,
            row_group_size=args.row_group_size,
            threads=args.threads,
            polars_streaming=args.polars_streaming,
            polars_sink_engine=args.polars_sink_engine,
            normal_sf=args.normal_sf,
            stress_small_repeats=args.stress_small_repeats,
            stress_big_repeats=args.stress_big_repeats,
        )

    elif args.cmd == "plot":
        out_dir = Path(args.out_dir)
        for test in ["normal", "stress-small", "stress-big"]:
            plot_test(out_dir, test=test, mode=args.mode)
        plot_overlay(out_dir, mode=args.mode)
        print(f"Wrote PNG files to: {out_dir}")

    elif args.cmd == "generate-string":
        scales = args.scales if args.scales else get_string_scales()
        ensure_s3_bucket_exists(args.bucket, region)
        generate_string_tables(
            bucket=args.bucket,
            prefix=args.prefix,
            region=region,
            endpoint_url=endpoint_url,
            row_group_size=args.row_group_size,
            threads=args.threads,
            scales=scales,
        )
        print("Done.")

    elif args.cmd == "run-string":
        tools = ["duckdb", "polars"] if args.tool == "all" else [args.tool]
        modes = ["join", "groupby"] if args.mode == "all" else [args.mode]
        out_dir = Path(args.out_dir)
        scales = args.scales if args.scales else get_string_scales()

        run_string_benchmarks(
            bucket=args.bucket,
            prefix=args.prefix,
            region=region,
            endpoint_url=endpoint_url,
            tools=tools,
            modes=modes,
            iterations=args.iterations,
            out_dir=out_dir,
            out_s3_prefix=args.out_s3_prefix,
            cleanup_s3_output=args.cleanup_s3_output,
            row_group_size=args.row_group_size,
            threads=args.threads,
            scales=scales,
        )

    elif args.cmd == "plot-string":
        out_dir = Path(args.out_dir)
        plot_string_benchmark(out_dir, "join")
        plot_string_benchmark(out_dir, "groupby")
        print(f"Wrote PNG files to: {out_dir}")

    else:
        die("Unknown command")


if __name__ == "__main__":
    main()
