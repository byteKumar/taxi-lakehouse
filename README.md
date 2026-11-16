# taxi-lakehouse

A batch and incremental data pipeline over the public NYC TLC trip records
(yellow and green taxi, monthly Parquet files). Raw files land in a local
lakehouse, dbt cleans and models them in DuckDB, data-quality tests gate each
run, and Dagster orchestrates the layers.

Work in progress. Run instructions and measured results will be added as the
milestones land.

Stack: Python 3.12, DuckDB, Parquet (Hive-style partitions), dbt-duckdb,
Dagster, pytest.
