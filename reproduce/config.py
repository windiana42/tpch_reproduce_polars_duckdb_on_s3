"""Shared configuration for reproduce scripts.

These settings match the largest SMALL datapoint from bench.env:
- TPC-H: stress-big sf=2.0 (2 files × sf=1.0 each)
- String tables: 1M rows

Configuration is loaded from bench.env (if it exists) and can be
overridden by environment variables.
"""

import os
from pathlib import Path
from urllib.parse import urlparse

import boto3


def _load_bench_env() -> None:
    """Load configuration from bench.env if it exists.

    Values are only set if not already present in environment,
    allowing environment variables to override bench.env.
    """
    # Find bench.env relative to this file (reproduce/config.py -> bench.env)
    config_dir = Path(__file__).resolve().parent.parent
    bench_env_path = config_dir / "bench.env"

    if not bench_env_path.exists():
        return

    with open(bench_env_path) as f:
        for line in f:
            line = line.strip()
            # Skip comments and empty lines
            if not line or line.startswith("#"):
                continue
            # Parse KEY=value
            if "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip()
            # Remove quotes from value
            if value and value[0] in ('"', "'") and value[-1] == value[0]:
                value = value[1:-1]
            # Only set if not already in environment
            if key and value and key not in os.environ:
                os.environ[key] = value


# Load bench.env before reading any configuration
_load_bench_env()

# ============================================================
# S3/MinIO connection (from bench.env / environment)
# ============================================================
ENDPOINT_URL = os.environ.get(
    "AWS_ENDPOINT_URL", ""
)  # e.g. "http://localhost:9000" for MinIO
ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "")
SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
REGION = os.environ.get("BENCH_AWS_REGION", "us-east-1")

# For DuckDB secret: strip http:// prefix
ENDPOINT = ENDPOINT_URL.replace("http://", "").replace("https://", "")

# ============================================================
# S3 paths (from bench.env / environment)
# ============================================================
BENCH_S3_BUCKET = os.environ.get("BENCH_S3_BUCKET", "benchmark")
BENCH_S3_PREFIX = os.environ.get("BENCH_S3_PREFIX", "tpch")
BENCH_OUT_S3_PREFIX = os.environ.get(
    "BENCH_OUT_S3_PREFIX", f"s3://{BENCH_S3_BUCKET}/outputs"
)

# TPC-H benchmark paths (largest SMALL datapoint: stress-big sf=2.0)
TPCH_PATHS = [
    f"s3://{BENCH_S3_BUCKET}/{BENCH_S3_PREFIX}/tpc/stress-big/lineitem_1.0_000.parquet",
    f"s3://{BENCH_S3_BUCKET}/{BENCH_S3_PREFIX}/tpc/stress-big/lineitem_1.0_001.parquet",
]

# String table paths (largest SMALL datapoint: 1M rows)
STRING_TABLE_A = (
    f"s3://{BENCH_S3_BUCKET}/{BENCH_S3_PREFIX}/string_tables/table_a_1000k.parquet"
)
STRING_TABLE_B = (
    f"s3://{BENCH_S3_BUCKET}/{BENCH_S3_PREFIX}/string_tables/table_b_1000k.parquet"
)

# Output paths
OUTPUT_PREFIX = f"{BENCH_OUT_S3_PREFIX}/reproduce"


def duckdb_s3_method() -> str:
    """Get DuckDB S3 access method from environment.

    Returns 'fsspec' or 'httpfs'. Default is 'fsspec' for airgapped environments.
    """
    return os.environ.get("BENCH_DUCKDB_S3_METHOD", "fsspec").lower()


def _get_extension_path(extension_name: str) -> str | None:
    """Get the path to a DuckDB extension from conda package.

    Args:
        extension_name: Name of the extension (e.g., "httpfs", "tpch")

    Returns the path if found, None otherwise.
    """
    import platform
    import sys

    import duckdb

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


def duckdb_connect_for_s3():
    """Create a DuckDB connection for S3 access.

    When using httpfs mode with extension from conda package, enables
    allow_unsigned_extensions (required for loading from non-default path).
    """
    import duckdb

    method = duckdb_s3_method()
    config: dict = {}

    if method == "httpfs":
        ext_path = _get_httpfs_extension_path()
        if ext_path:
            # Loading from conda package requires unsigned extensions
            config["allow_unsigned_extensions"] = True

    return duckdb.connect(database=":memory:", config=config)


def duckdb_setup_s3(con) -> None:
    """Configure DuckDB connection for S3 access.

    Uses fsspec (default) or httpfs based on BENCH_DUCKDB_S3_METHOD env var.
    """
    method = duckdb_s3_method()

    if method == "fsspec":
        _duckdb_setup_s3_fsspec(con)
    else:
        _duckdb_setup_s3_httpfs(con)


def _clear_fsspec_s3_config() -> None:
    """Clear any existing fsspec S3 configuration to avoid pollution."""
    try:
        from fsspec.config import conf
        if "s3" in conf:
            conf.pop("s3")
    except ImportError:
        pass


def _duckdb_setup_s3_fsspec(con) -> None:
    """Setup DuckDB with fsspec/s3fs (no extension needed)."""
    import fsspec
    from fsspec.config import conf

    # Clear any existing config to avoid pollution between runs
    _clear_fsspec_s3_config()

    conf.setdefault("s3", {})
    s3_conf: dict = {
        "key": ACCESS_KEY,
        "secret": SECRET_KEY,
        "config_kwargs": {
            "connect_timeout": 3,
            "read_timeout": 5,
            "retries": {"max_attempts": 2, "mode": "standard"},
        },
    }
    if ENDPOINT_URL:
        s3_conf["client_kwargs"] = {"endpoint_url": ENDPOINT_URL}
        s3_conf["config_kwargs"]["s3"] = {"addressing_style": "path"}
    if REGION:
        s3_conf.setdefault("client_kwargs", {})["region_name"] = REGION
    conf["s3"].update(s3_conf)

    fs = fsspec.filesystem("s3")
    con.register_filesystem(fs)


def _duckdb_setup_s3_httpfs(con) -> None:
    """Setup DuckDB with HTTPFS extension.

    Tries to load from duckdb-extension-httpfs conda package first. Falls back to
    INSTALL httpfs if package not available.
    """
    # Clear any fsspec config to avoid polluting Polars S3 access
    _clear_fsspec_s3_config()

    # Load from conda package (duckdb-extension-httpfs)
    ext_path = _get_httpfs_extension_path()
    if not ext_path:
        import duckdb
        raise RuntimeError(
            f"httpfs extension not found for DuckDB {duckdb.__version__}. "
            "Ensure duckdb-extension-httpfs conda package is installed and versions match."
        )

    con.execute(f"LOAD '{ext_path}';")

    secret_opts = ["TYPE s3"]
    if ENDPOINT:
        secret_opts.append(f"KEY_ID '{ACCESS_KEY}'")
        secret_opts.append(f"SECRET '{SECRET_KEY}'")
        secret_opts.append(f"ENDPOINT '{ENDPOINT}'")
        if ENDPOINT_URL.startswith("http://"):
            secret_opts.append("USE_SSL false")
        secret_opts.append("URL_STYLE path")
    else:
        secret_opts.append("PROVIDER credential_chain")
    secret_opts.append(f"REGION '{REGION}'")

    con.execute(f"CREATE SECRET ({', '.join(secret_opts)});")


def polars_storage_options() -> dict:
    """Build Polars storage options for S3 access."""
    opts = {
        "aws_access_key_id": ACCESS_KEY,
        "aws_secret_access_key": SECRET_KEY,
        "aws_region": REGION,
    }
    if ENDPOINT_URL:
        opts["endpoint_url"] = ENDPOINT_URL
        if ENDPOINT_URL.startswith("http://"):
            opts["aws_allow_http"] = "true"
    return opts


def cleanup_s3_path(s3_path: str) -> int:
    """Delete S3 file or all objects under a prefix.

    Returns count of deleted objects.
    """
    parsed = urlparse(s3_path)
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")

    client_kwargs = {
        "aws_access_key_id": ACCESS_KEY,
        "aws_secret_access_key": SECRET_KEY,
        "region_name": REGION,
    }
    if ENDPOINT_URL:
        client_kwargs["endpoint_url"] = ENDPOINT_URL

    s3 = boto3.client("s3", **client_kwargs)

    # List and delete all objects with this prefix
    deleted = 0
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        objects = page.get("Contents", [])
        if objects:
            s3.delete_objects(
                Bucket=bucket,
                Delete={"Objects": [{"Key": obj["Key"]} for obj in objects]},
            )
            deleted += len(objects)
    return deleted
