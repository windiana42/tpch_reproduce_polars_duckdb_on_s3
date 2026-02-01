#!/bin/bash
# Benchmark activation script
# Sources configuration from bench.env and sets up DuckDB extension loading.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_ROOT="$(dirname "$SCRIPT_DIR")"

# =============================================================================
# Load configuration from bench.env
# =============================================================================
# bench.env contains all S3 credentials, bucket paths, and benchmark settings.
# If it doesn't exist, defaults are used (set below or from environment).
if [[ -f "$BENCH_ROOT/bench.env" ]]; then
    # Source bench.env, but only export non-empty values
    # This allows environment variables to override bench.env if set
    while IFS='=' read -r key value; do
        # Skip comments and empty lines
        [[ "$key" =~ ^#.*$ || -z "$key" ]] && continue
        # Remove leading/trailing whitespace and quotes from value
        value="${value#\"}"
        value="${value%\"}"
        value="${value#\'}"
        value="${value%\'}"
        # Only set if not already set in environment (allows override)
        if [[ -z "${!key}" && -n "$value" ]]; then
            export "$key=$value"
        fi
    done < "$BENCH_ROOT/bench.env"
fi

# =============================================================================
# DuckDB extension loading workaround
# =============================================================================
# python-duckdb has DuckDB statically linked with hidden symbols,
# but extensions need libduckdb symbols - preloading the shared library fixes this.

# If we're using the DuckDB CLI backend (subprocess), don't preload anything.
# The CLI should have a consistent runtime (libduckdb) with its extensions.
if [[ "${BENCH_DUCKDB_BACKEND:-python}" != "cli" ]]; then
    if [[ "$OSTYPE" == "darwin"* ]]; then
        export DYLD_INSERT_LIBRARIES="$CONDA_PREFIX/lib/libduckdb.dylib"
    elif [[ "$OSTYPE" == "linux"* ]]; then
        # IMPORTANT: LD_PRELOAD on Linux can cause crashes (double free).
        # Only enable if BENCH_DUCKDB_LD_PRELOAD=1 is explicitly set.
        if [[ "${BENCH_DUCKDB_LD_PRELOAD:-0}" == "1" ]]; then
            export LD_PRELOAD="$CONDA_PREFIX/lib/libduckdb.so"
        fi
    fi
fi
