# Implementation Plan: Fix DeltaScanConfig Schema and File Column Bugs

## Objectives
1. Respect `DeltaScanConfig::schema` by properly passing down types and adding physical projection / casting.
2. Enable `insert_into` to work properly with `DeltaScanConfig::schema` by accepting the configured types rather than strictly enforcing physical types.
3. Fix the `retain_file_ids` bug so the `file_column_name` is correctly pruned if it's not present in DataFusion's logical query projection.
4. Adopt modular helper functions for scan filtering and pushdowns.
5. Apply casting via DataFusion 52's physical execution expressions rather than full `SchemaAdapter` re-implementations.
6. Fix the file column bug last.
7. Separate the work into nice, logical commits in accordance with `@CONTRIBUTING.md`.

## Commit Plan

### Commit 1: Modularize Filter Pushdown and Scan Filtering
* **Goal**: Refactor filter generation and pushdown into a set of clean helper functions, making the scan setup more robust and understandable before addressing schema issues.
* **Changes**:
  * Move predicate analysis (identifying which filters can be pushed to Parquet, which to Delta kernel) into modular helper functions within `crates/core/src/delta_datafusion/table_provider/next/mod.rs` or `scan/mod.rs`.
  * Ensure the logic for parsing and mapping filters cleanly separates expression evaluation and kernel predicate creation.
  * Extract the masking of filters during scan and filter pushdown into these helpers.

### Commit 2: Respect `DeltaScanConfig::schema` in `DeltaScanNext` execution
* **Goal**: Use DataFusion 52 `PhysicalExprAdapter` and `PhysicalExprSimplifier` principles to cast comparing literals instead of whole columns, and properly pass the logical schema through the physical plan.
* **Changes**:
  * Update `KernelScanPlan` and `DeltaScanNext` to retain and respect `config.schema`.
  * Adapt `get_read_plan` (or equivalent scan executor) to project to the configured logical schema.
  * Add the necessary `PhysicalExprAdapter` to cast comparing literals to match physical data types, ensuring pushdown works smoothly even when logical and physical schemas differ.

### Commit 3: Fix `insert_into` schema validation for logical schemas
* **Goal**: Ensure the internal `DeltaDataSink` accepts writes conforming to `DeltaScanConfig::schema`.
* **Changes**:
  * Pass the logical schema (if provided) to the `DeltaDataSink` instead of just the strict `snapshot.read_schema()`.
  * Update `insert_into` in `TableProvider` for `DeltaScanNext` to use the provided schema for validation, converting appropriately before committing the snapshot.

### Commit 4: Fix file column projection bug (`retain_file_ids`)
* **Goal**: Ensure the internal file ID column is only returned if it is part of the DataFusion query projection.
* **Changes**:
  * Update `DeltaScanNext::scan()` where `retain_file_ids` is determined.
  * Currently, it is unconditionally set to `true` if `config.file_column_name` is present.
  * Change it to: Check if the column exists in the `projection` argument passed from DataFusion. If the query does not ask for it, set `retain_file_ids` to `false`, allowing the column to be correctly projected out.

## Action Plan
1. `cargo fmt -- --check` prior to every code change and commit.
2. Develop the modular filter helper logic. Run tests. Commit.
3. Develop the physical expression literal casting logic and schema integration. Run tests. Commit.
4. Develop the DataSink schema fix. Run tests. Commit.
5. Develop the file column projection logic. Run tests. Commit.

