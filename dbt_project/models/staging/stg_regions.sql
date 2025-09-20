{{ config(materialized='table', alias='regions') }}

{% set load_date = var('load_date', run_started_at.strftime('%Y-%m-%d')) %}

with raw as (
    select * exclude({% for col in var('exclude_cols') %} {{ col }}, {% endfor %}) 
    from {{ ref('raw_regions') }}
),

deduped as (
    select distinct *  
    from raw
),

cleaned as (
    select
        lower(slug) as slug,
        name as name,
        parent as parent,
    from deduped
),

final as (
    select
        *,
        cast('{{load_date}}' as date) as load_date
    from cleaned
)

select * from final
