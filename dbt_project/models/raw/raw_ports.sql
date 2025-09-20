{{ config(
    materialized='table',
    alias='ports'
) }}

{% set load_type = 'full_load' %}
{% set load_date = var('load_date', run_started_at.strftime('%Y-%m-%d')) %}
{% set file_path = var('file_path', "../input_files/" ~ load_type ~ "/DE_casestudy_ports.csv") %}

with src as (
  select 
    *
  from read_csv_auto('{{file_path}}')
),

final as (
    select
        *,
        cast('{{file_path}}' as varchar) AS file_path,
        cast('{{load_type}}' as varchar) AS load_type,
        cast('{{load_date}}' as date) AS load_date
    from src
)

select * from final
