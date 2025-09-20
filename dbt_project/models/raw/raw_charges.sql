{{ config(
    unique_key='d_id',
    strategy='delete+insert',
    alias='charges'
) }}

{% set load_type = 'incremental' if is_incremental() else 'full_load' %}
{% set load_date = var('load_date', run_started_at.strftime('%Y-%m-%d')) %}
{% set file_path = var('file_path', "../input_files/" ~ load_type ~ "/DE_casestudy_charges*.csv") %}

with src as (
    select 
        *
    from read_csv_auto('{{file_path}}')
    {% if is_incremental() %}
        where d_id not in (select distinct d_id from {{ this }})
         -- where load_date > (select coalesce(max(load_date), date '1900-01-01') from {{ this }})
    {% endif %}
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
