{#
Partition-level incremental processing ("insert overwrite by month").

A partition is one (service, year_month) raw file. Each incremental model
keeps a high-water mark in meta.partition_watermark: the bronze load_id it
last processed for every partition. On an incremental run a partition is
stale when bronze.load_log has a newer loaded/replaced load_id for it (a new
month, or a month whose file TLC republished). Only stale partitions are read,
deleted from the target and re-inserted.

Override with --vars '{"months": ["2026-09"]}' to force specific months for
both services (this is what Dagster partitions and backfills use).
#}

{% macro latest_loads_sql() %}
    select service, year_month, max(load_id) as load_id
    from bronze.load_log
    where status in ('loaded', 'replaced')
    group by service, year_month
{% endmacro %}

{# Returns a list of 'service|YYYY-MM' strings, or none meaning "all". #}
{% macro partitions_to_process(model_name) %}
    {% if not execute %}{{ return([]) }}{% endif %}

    {% set forced = var('months', none) %}
    {% if forced is not none %}
        {% if forced is string %}{% set forced = forced.split(',') %}{% endif %}
        {% set out = [] %}
        {% for m in forced %}
            {% for s in ['yellow', 'green'] %}{% do out.append(s ~ '|' ~ (m | trim)) %}{% endfor %}
        {% endfor %}
        {{ return(out) }}
    {% endif %}

    {% if not is_incremental() %}{{ return(none) }}{% endif %}

    {% set q %}
        with latest as ({{ latest_loads_sql() }}),
        done as (
            select service, year_month, max(load_id) as load_id
            from meta.partition_watermark where model = '{{ model_name }}'
            group by service, year_month
        )
        select l.service || '|' || l.year_month
        from latest l left join done d using (service, year_month)
        where d.load_id is null or l.load_id > d.load_id
        order by 1
    {% endset %}
    {{ return(run_query(q).columns[0].values() | list) }}
{% endmacro %}

{#
SQL predicate selecting the partitions. mode='source' filters the integer
Hive columns (src_year, src_month) so DuckDB prunes files at the scan;
mode='target' filters a materialized table by service_type and source_month.
#}
{% macro partition_predicate(parts, mode='source') %}
    {%- if parts is none -%} true
    {%- elif parts | length == 0 -%} false
    {%- else -%}
        ({%- for p in parts -%}
            {%- set s, ym = p.split('|') -%}
            {%- set y, m = ym.split('-') -%}
            {%- if mode == 'source' -%}
                (service_type = '{{ s }}' and src_year = {{ y | int }} and src_month = {{ m | int }})
            {%- else -%}
                (service_type = '{{ s }}' and source_month = date '{{ ym }}-01')
            {%- endif -%}
            {%- if not loop.last %} or {% endif -%}
        {%- endfor -%})
    {%- endif -%}
{% endmacro %}

{# pre_hook: on incremental runs delete the partitions about to be rewritten;
   on full refresh forget this model's watermarks. #}
{% macro overwrite_partitions_pre_hook() %}
    {% if is_incremental() %}
        delete from {{ this }}
        where {{ partition_predicate(partitions_to_process(this.name), 'target') }}
    {% else %}
        delete from meta.partition_watermark where model = '{{ this.name }}'
    {% endif %}
{% endmacro %}

{# post_hook: record which bronze load each processed partition came from. #}
{% macro record_watermark_post_hook(parts) %}
    insert into meta.partition_watermark
    select '{{ this.name }}', service, year_month, load_id, now()
    from ({{ latest_loads_sql() }}) l
    where
    {%- if parts is none %} true
    {%- elif parts | length == 0 %} false
    {%- else %} l.service || '|' || l.year_month in (
        {%- for p in parts %}'{{ p }}'{% if not loop.last %}, {% endif %}{% endfor -%})
    {%- endif %}
{% endmacro %}
