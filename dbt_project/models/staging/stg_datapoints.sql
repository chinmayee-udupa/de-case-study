{{
    config(
        unique_key='datapoint_id',
        strategy='merge',
        alias='datapoints'
    )
}}

{% set load_date = var('load_date', run_started_at.strftime('%Y-%m-%d')) %}

with raw as (
    select * from {{ ref('raw_datapoints') }}
    {% if is_incremental() %}
      -- and d_id not in (select datapoint_id from {{ this }})
      where load_date > (select max(load_date) from {{ this }})
    {% endif %}
),

deduped as (
    select
        *,
        row_number() over (partition by d_id order by created desc, load_date desc) as rn
    from raw
),

cleaned as (
    select
        d_id as datapoint_id,
        cast(created as timestamp) as created,
        origin_pid as origin_port_id,
        destination_pid as destination_port_id,
        cast(valid_from as date) as valid_from,
        cast(valid_to as date) as valid_to,
        company_id as company_id,
        supplier_id as supplier_id,
        equipment_id as equipment_id
    from deduped
    where rn = 1
    and valid_from <= valid_to
),

-- keep only 1 row per d_id
final as (
  select 
    *, 
    cast('{{load_date}}' as date) as load_date
  from cleaned
)

select * from final
