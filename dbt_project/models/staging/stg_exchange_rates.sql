{{
    config(
        materialized='table',
        alias='exchange_rates'
    )
}}

{% set load_date = var('load_date', run_started_at.strftime('%Y-%m-%d')) %}

with raw as (
    select * from {{ ref('raw_exchange_rates') }}
      {#
      {% if is_incremental() %}
      -- only bring new exchange rates
      -- and (day, currency) not in (select day, currency from {{ this }})
        where load_date > (select max(load_date) from {{ this }})
      {% endif %}
      #}
),

deduped as (
    select
        *,
        row_number() over (
            partition by day, currency
            order by day desc
        ) as rn
    from raw
),

final as (
    select 
        day as exchange_date,
        currency as currency,
        rate as rate,
        cast('{{load_date}}' as date) as load_date
    from deduped
    where rn = 1
)

select * from final
