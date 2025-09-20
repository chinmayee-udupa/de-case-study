{{ config(
    pre_hook=[
        "{{ snapshot_coverage('before') }}"
    ],
    materialized='incremental',
    unique_key=['valid_day', 'equipment_id', 'origin_port_id', 'destination_port_id'],
    alias='daily_lane_equipment_prices',
    post_hook=[
        "{{ snapshot_coverage('after') }}"
    ]
) }}

-- 1. datapoints_daily_expanded: Expands each shipping contract (datapoint)
--    into a record for every single day it is valid, from 'valid_from' to 'valid_to'.
--    This addresses the requirement to "Identify which datapoints are valid for each day".
with datapoints as (
    select * from {{ ref('stg_datapoints') }}
),

{% if is_incremental() %}
  new_window as (
      select
          min(valid_from) as min_valid_from,
          max(valid_to)   as max_valid_to
      from datapoints
      where created > (select max(created) from {{ this }})
  ),
{% endif %}

charges as (
    select * from {{ ref('stg_charges') }}
),

datapoints_daily_expanded as (
    select
        sd.datapoint_id,
        sd.created,
        sd.company_id,
        sd.supplier_id,
        sd.origin_port_id,
        sd.destination_port_id,
        sd.equipment_id,
        cast(gs.valid_day as date) as valid_day
    from {{ ref('stg_datapoints') }} sd
    join generate_series(sd.valid_from, sd.valid_to, interval 1 day) as gs(valid_day)
        on true
    where sd.valid_from <= sd.valid_to
    {% if is_incremental() %}
        and created > (select max(created) from {{ this }})
        or cast(gs.valid_day as date) between (select min_valid_from from new_window)
                   and (select max_valid_to from new_window)
    {% endif %}
),

-- 2. datapoint_daily_charges_usd: Calculates the USD value for each individual charge
--    on each valid day. This is crucial for "Calculate the price level of each datapoint in USD each day".
--    It joins with exchange rates based on the specific day and currency.
datapoint_daily_charges_usd as (
    select
        dde.datapoint_id,
        dde.created,
        dde.company_id,
        dde.supplier_id,
        dde.origin_port_id,
        dde.destination_port_id,
        dde.equipment_id,
        dde.valid_day,
        sc.currency,
        sc.charge_value,
        ser.rate as exchange_rate,
        -- Convert charge value to USD using the daily exchange rate
        -- Per source [1], example conversion is `USD_value` = `EUR value` / `"rate"`
        -- Handle cases where exchange rate might be missing or zero to prevent errors
        case
            -- Mark as null if conversion isn't possible
            when ser.rate is null or ser.rate = 0 then null
            else sc.charge_value / ser.rate
        end as charge_value_usd
    from datapoints_daily_expanded dde
    join charges sc
        on dde.datapoint_id = sc.datapoint_id
    left join {{ ref('stg_exchange_rates') }} ser
        on
            dde.valid_day = ser.exchange_date
            and sc.currency = ser.currency
),

-- 3. datapoint_daily_total_usd_price: Sums up all individual USD-converted charges
--    to get the total daily price for each shipping contract (datapoint).
datapoint_daily_total_usd_price as (
    select
        datapoint_id,
        max(created) as created,
        company_id,
        supplier_id,
        origin_port_id,
        destination_port_id,
        equipment_id,
        valid_day,
        sum(charge_value_usd) as total_usd_price
    from datapoint_daily_charges_usd
    -- Exclude charges that couldn't be converted to USD
    where charge_value_usd is not null
    group by
        datapoint_id,
        company_id,
        supplier_id,
        origin_port_id,
        destination_port_id,
        equipment_id,
        valid_day
),

-- 4. datapoint_full_lane_info: Enriches the daily total USD prices with comprehensive
--    lane information (ports and regions) by joining with 'stg_ports' and 'stg_regions'.
--    This prepares the data for aggregation by "any two port(s) or region(s)".
datapoint_full_lane_info as (
    select
        ddtup.datapoint_id,
        ddtup.created,
        ddtup.company_id,
        ddtup.supplier_id,
        ddtup.equipment_id,
        ddtup.valid_day,
        ddtup.total_usd_price,

        -- Origin Port and Region Information
        spo.port_id as origin_port_id,
        spo.code as origin_port_code,
        spo.name as origin_port_name,
        spo.region_name as origin_region_name,
        spo.country as origin_country,
        spo.country_code as origin_country_code,

        -- Destination Port and Region Information
        spd.port_id as destination_port_id,
        spd.code as destination_port_code,
        spd.name as destination_port_name,
        spd.region_name as destination_region_name,
        spd.country as destination_country,
        spd.country_code as destination_country_code
        
    from datapoint_daily_total_usd_price ddtup
    join {{ ref('stg_ports') }} spo
        on ddtup.origin_port_id = spo.port_id
    join {{ ref('stg_ports') }} spd
        on ddtup.destination_port_id = spd.port_id
),

-- 5. final_aggregation_and_dq: Performs the final aggregation (average and median prices)
--    and calculates the counts for distinct companies and suppliers needed for the DQ check.
final_aggregation_and_dq as (
    select
        fli.valid_day,
        fli.equipment_id,

        -- Grouping by lane identifiers (both port and region level)
        fli.origin_port_id,
        fli.origin_port_code,
        fli.origin_port_name,
        fli.origin_region_name,
        fli.origin_country_code,

        fli.destination_port_id,
        fli.destination_port_code,
        fli.destination_port_name,
        fli.destination_region_name,
        fli.destination_country_code,

        -- Calculate average and median daily prices in USD
        avg(fli.total_usd_price) as avg_price_usd,
        -- DuckDB supports MEDIAN directly
        median(fli.total_usd_price) as median_price_usd,

        -- Count distinct companies and suppliers for the data quality check
        count(distinct fli.company_id) as distinct_companies_count,
        count(distinct fli.supplier_id) as distinct_suppliers_count,

        max(created) as created -- Keep track of the most recent 'created' timestamp

    from datapoint_full_lane_info fli
    group by
        fli.valid_day,
        fli.equipment_id,
        fli.origin_port_id,
        fli.origin_port_code,
        fli.origin_port_name,
        fli.origin_region_name,
        fli.origin_country_code,
        fli.destination_port_id,
        fli.destination_port_code,
        fli.destination_port_name,
        fli.destination_region_name,
        fli.destination_country_code
)

-- 6. Final Selection: Selects all aggregated columns and adds the 'dq_ok' flag.
--    The 'dq_ok' column is a boolean indicating "sufficient coverage" per source [2, 3].
select
    valid_day,
    equipment_id,
    concat(origin_port_name, ' to ', destination_port_name) as port_lane,

    origin_port_id,
    origin_port_code,
    origin_port_name,
    origin_region_name,
    origin_country_code,

    destination_port_id,
    destination_port_code,
    destination_port_name,
    destination_region_name,
    destination_country_code,

    -- Round prices to 2 decimal places for currency
    round(avg_price_usd, 2) as avg_price_usd,
    round(median_price_usd, 2) as median_price_usd,
    distinct_companies_count,
    distinct_suppliers_count,

    -- Implement the 'dq_ok' boolean column: True if at least 5 different companies
    -- and 2 different suppliers provide data for the given lane, equipment type, and day.
    {{ dq_ok_check(5, 2) }} as dq_ok,
    created,
    current_date as load_date

from final_aggregation_and_dq
order by
    valid_day,
    equipment_id,
    origin_port_id,
    destination_port_id
