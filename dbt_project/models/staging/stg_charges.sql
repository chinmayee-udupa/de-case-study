{{ config(
    unique_key='datapoint_id',
    strategy='delete+insert',
    alias='charges'
) }}

{% set load_date = var('load_date', run_started_at.strftime('%Y-%m-%d')) %}

with raw as (
    select * exclude({% for col in var('exclude_cols') %} {{ col }}, {% endfor %}) 
    from {{ ref('raw_charges') }}
    {% if is_incremental() %}
      -- where d_id not in (select distinct datapoint_id from {{ this }})
      where load_date > (select max(load_date) from {{ this }})
    {% endif %}
),

cleaned as (
    select
        d_id as datapoint_id,
        trim(currency) as currency,
        charge_value as charge_value,
    from raw
    where currency is not null or currency != ''
),

final as (
    select
        *, 
        cast('{{load_date}}' as date) as load_date
    from cleaned
)

select * from final
