{% macro snapshot_coverage(stage) %}
  {% if execute %}

    -- Ensure metadata_coverage table exists
    {% set create_table_sql %}
      create table if not exists final.metadata_coverage (
        snapshot_ts timestamp,
        stage varchar,
        covered_lane_count bigint
      )
    {% endset %}
    {% do run_query(create_table_sql) %}
    
    {% if is_incremental() %}
    -- Target table for coverage snapshot
    {% set target_table = this %}

    -- Insert coverage snapshot
    {% set insert_sql %}
      insert into final.metadata_coverage (
        snapshot_ts,
        stage,
        covered_lane_count
      )
      select
        current_timestamp as snapshot_ts,
        '{{ stage }}' as stage,
        count(*) as covered_lane_count
      from {{ target_table }}
      where dq_ok = true
      --distinct concat_ws('-', origin_port_id, destination_port_id, equipment_id, valid_day)
    {% endset %}

    {% do run_query(insert_sql) %}

    {% endif %}
  {% endif %}
{% endmacro %}
