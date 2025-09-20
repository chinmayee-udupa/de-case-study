{{ config(
    unique_key=['d_id'],
    strategy='merge',
    alias='datapoints'
) }}

{% set load_type = 'incremental' if is_incremental() else 'full_load' %}
{% set load_date = var('load_date', run_started_at.strftime('%Y-%m-%d')) %}
{% set file_path = var('file_path', "../input_files/" ~ load_type ~ "/DE_casestudy_datapoints*.csv") %}

with src as (
  select 
    *
  from read_csv_auto('{{file_path}}')
    {% if is_incremental() %}
      -- where d_id not in (select d_id from {{ this }})
      where created > (select max(created) from {{ this }})
    {% endif %}
),

final as (
    select
        *, 
        cast('{{file_path}}' as varchar) as file_path,
        cast('{{load_type}}' as varchar) as load_type,
        cast('{{load_date}}' as date) as load_date
    from src
)

select * from final
