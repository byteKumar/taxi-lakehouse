{# Read one service's raw monthly files in place. The Hive folder names
   (service=/year=/month=) become columns; filters on year and month let
   DuckDB skip whole files. Types are given explicitly because auto-detection
   reads month=09 as VARCHAR. union_by_name fills columns that only exist in
   some months (cbd_congestion_fee from 2025-01, request_source from 2026-06)
   with NULL. #}
{% macro raw_dir() -%}
    {{ return(var('raw_dir', env_var('TAXI_RAW_DIR', '../data/raw'))) }}
{%- endmacro %}

{# Columns that only exist in some months. union_by_name only knows about
   columns in the files actually present, so a raw dir holding just pre-2025
   files has no cbd_congestion_fee at all. The empty typed branch below adds
   any missing ones as NULL (UNION ALL BY NAME), so staging always binds. #}

{% macro bronze(service) -%}
(
    select * from read_parquet(
        '{{ raw_dir() }}/service={{ service }}/year=*/month=*/*.parquet',
        hive_partitioning = true,
        hive_types = {'service': 'VARCHAR', 'year': 'INTEGER', 'month': 'INTEGER'},
        union_by_name = true,
        file_row_number = true
    )
    union all by name
    select
    {%- for name, type in bronze_optional_columns(service).items() %}
        cast(null as {{ type }}) as {{ name }}{% if not loop.last %},{% endif %}
    {%- endfor %}
    where false
)
{%- endmacro %}

{% macro bronze_optional_columns(service) -%}
    {{ return({
        'yellow': {'Airport_fee': 'DOUBLE', 'cbd_congestion_fee': 'DOUBLE', 'request_source': 'VARCHAR'},
        'green':  {'ehail_fee': 'DOUBLE', 'cbd_congestion_fee': 'DOUBLE', 'request_source': 'VARCHAR'},
    }[service]) }}
{%- endmacro %}
