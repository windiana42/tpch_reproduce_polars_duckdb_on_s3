#!/usr/bin/env python3
"""Run all reproduce benchmarks (single datapoint each)."""

print("=" * 60)
print("Reproduce Benchmarks - Single Datapoint Each")
print("=" * 60)

print("\n--- filter_write ---")
import filter_write

duck1 = filter_write.run_duckdb()
polar1 = filter_write.run_polars()

print("\n--- copy_project ---")
import copy_project

duck2 = copy_project.run_duckdb()
polar2 = copy_project.run_polars()

print("\n--- join ---")
import join

duck3 = join.run_duckdb()
polar3 = join.run_polars()

print("\n--- groupby ---")
import groupby

duck4 = groupby.run_duckdb()
polar4 = groupby.run_polars()

print("\n" + "=" * 60)
print("Summary")
print("=" * 60)
print(f"{'Benchmark':<20} {'DuckDB':>10} {'Polars':>10} {'Ratio':>10}")
print("-" * 50)
print(f"{'filter_write':<20} {duck1:>9.3f}s {polar1:>9.3f}s {polar1 / duck1:>9.1f}x")
print(f"{'copy_project':<20} {duck2:>9.3f}s {polar2:>9.3f}s {polar2 / duck2:>9.1f}x")
print(f"{'join':<20} {duck3:>9.3f}s {polar3:>9.3f}s {polar3 / duck3:>9.1f}x")
print(f"{'groupby':<20} {duck4:>9.3f}s {polar4:>9.3f}s {polar4 / duck4:>9.1f}x")
