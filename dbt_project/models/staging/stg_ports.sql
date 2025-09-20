{{ config(materialized='table', alias='ports') }}

{% set load_date = var('load_date', run_started_at.strftime('%Y-%m-%d')) %}

with raw as (
    select * exclude({% for col in var('exclude_cols') %} {{ col }}, {% endfor %}) 
    from {{ ref('raw_ports') }}
),

deduped as (
    select distinct *
    from raw
),

cleaned as (
    select
        pid as port_id,
        upper(code) as code,
        lower(deduped.slug) as slug,
        deduped.name as name,
        country as country,
        upper(country_code) as country_code,
        r.name as region_name
    from deduped
    join {{ ref('stg_regions') }} r
        on deduped.slug = r.slug
),

final as (
    select
        *,
        cast('{{load_date}}' as date) as load_date
    from cleaned
)

select * from final
