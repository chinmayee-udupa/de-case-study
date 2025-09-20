# XENETA SHIPPING DATA

## CORE FILTERS FOR EVERY QUERY
Always consider filtering by these core dimensions for relevant results:
- `valid_day` or a date range (`BETWEEN 'start_date' AND 'end_date'`)
- `equipment_id` (Container Type: 1-6)
- `dq_ok = TRUE` (to ensure data quality)

```sql lane_prices_by_day
SELECT
    *
FROM final.daily_lane_equipment_prices
```

```sql equipment_types
select
    equipment_id
from final.equipment_type
group by equipment_id
order by equipment_id
```

<Dropdown data={equipment_types} name=equipment_type value=equipment_id>
  <DropdownOption value=null valueLabel="Select Equipment"/>
</Dropdown>

<DateInput
    name=range_filtering_a_query
    data={lane_prices_by_day}
    dates=valid_day
    title='Date Range'
    range
/>

```sql port_codes
select
    code
from final.ports
group by code
order by code
```

```sql lanes
select
    concat(origin_port_id, ' - ', origin_port_id) as lane_description,
from final.datapoints
group by origin_port_id, origin_port_id
order by lane_description
```

<Dropdown data={port_codes} name=o_port_code value=code>
  <DropdownOption value="%" valueLabel="Select Origin Port"/>
</Dropdown>

<Dropdown data={port_codes} name=d_port_code value=code>
  <DropdownOption value="%" valueLabel="Select Destination Port"/>
</Dropdown>

<Dropdown data={lanes} name=lane value=lane_description>
  <DropdownOption value="%" valueLabel="Select Lane"/>
</Dropdown>

## 1. PRICE TIME SERIES FOR A SPECIFIC PORT-TO-PORT LANE
-- Use this to track price movements for a critical route.
```sql port_to_port_lane
SELECT
    valid_day,
    avg_price_usd,
    median_price_usd
FROM ${lane_prices_by_day}
WHERE
    origin_port_code like '${inputs.o_port_code.value}' -- e.g., Yantian
    AND destination_port_code like '${inputs.d_port_code.value}' -- e.g., Los Angeles
    AND equipment_id = coalesce(${inputs.equipment_type.value}, equipment_id) -- Dry Container
    AND dq_ok = TRUE -- Reliable data only
    AND valid_day BETWEEN '${inputs.range_filtering_a_query.start}' and '${inputs.range_filtering_a_query.end}' -- Q1 2022
ORDER BY valid_day;
```

<LineChart
  data={port_to_port_lane}
  x="valid_day"
  y="avg_price_usd"
  title="Port-to-Port Lane Average Price (USD)"
  subtitle="Price trend for the selected lane {inputs.o_port_code.value} to {inputs.d_port_code.value} and equipment_id = {inputs.equipment_type.value} and dq_ok = TRUE"
/>

<BarChart 
  data={port_to_port_lane} 
  x="valid_day" 
  y="avg_price_usd" 
  title="Port-to-Port Lane Average Price (USD)" 
  subtitle="Price trend for the selected lane {inputs.o_port_code.value} to {inputs.d_port_code.value} and equipment_id = {inputs.equipment_type.value} and dq_ok = TRUE" 
/>

## 2. COMPARE AVERAGE vs. MEDIAN PRICE FOR A LANE
-- A large gap indicates a skewed price distribution (a few very high/low contracts).
```sql avg_vs_median_comparison
SELECT
    valid_day,
    avg_price_usd,
    median_price_usd,
    (avg_price_usd - median_price_usd) AS avg_median_diff
FROM ${lane_prices_by_day}
WHERE
    origin_port_code like '${inputs.o_port_code.value}'
    AND destination_port_code like '${inputs.d_port_code.value}'
    AND equipment_id = coalesce(${inputs.equipment_type.value}, equipment_id)
    AND dq_ok = TRUE
ORDER BY valid_day;
```

<BarChart 
  data={avg_vs_median_comparison} 
  x="valid_day"
  y="avg_median_diff"
  title="Average vs Median Price Difference (USD)" 
/>

```sql region_names
select
    name
from final.regions
group by name
```

<Dropdown data={region_names} name=o_region_name value=name>
  <DropdownOption value="%" valueLabel="Select Origin Region"/>
</Dropdown>

<Dropdown data={region_names} name=d_region_name value=name>
  <DropdownOption value="%" valueLabel="Select Destination Region"/>
</Dropdown>

## 3. FIND AVERAGE PRICE BETWEEN TWO REGIONS
-- This aggregates all port-to-port lanes between two regions.
```sql region_to_region
SELECT
    valid_day,
    AVG(avg_price_usd) AS region_avg_price_usd, -- Average of the port-level averages
    COUNT(*) AS number_of_underlying_lanes -- How many port-pairs contribute
FROM ${lane_prices_by_day}
WHERE
    origin_region_name like '${inputs.o_region_name.value}'
    AND destination_region_name like '${inputs.d_region_name.value}'
    AND equipment_id = coalesce(${inputs.equipment_type.value}, equipment_id)
    AND dq_ok = TRUE -- CRITICAL: Only use reliable lanes for this calculation
    AND valid_day BETWEEN '${inputs.range_filtering_a_query.start}' and '${inputs.range_filtering_a_query.end}' -- Or use a date range
GROUP BY valid_day
ORDER BY valid_day;
```

<BarChart 
  data={region_to_region} 
  x="valid_day" 
  y="region_avg_price_usd" 
  title="Average vs Median Price Difference (USD)" 
/>

## 4. IDENTIFY THE MOST EXPENSIVE/ACTIVE LANES ON A DATE RANGE
-- Perfect for market overview and spotting outliers.
```sql most_expensive_lanes
SELECT
    origin_port_name,
    origin_region_name,
    destination_port_name,
    destination_region_name,
    avg_price_usd,
    concat(origin_port_name, ' to ', destination_port_name) as lane_description
FROM ${lane_prices_by_day}
WHERE
    valid_day BETWEEN '${inputs.range_filtering_a_query.start}' and '${inputs.range_filtering_a_query.end}'
    AND dq_ok = TRUE
    AND equipment_id = coalesce(${inputs.equipment_type.value}, equipment_id) -- Adjust or remove filter for all types
ORDER BY avg_price_usd DESC
LIMIT 20;
```

<BarChart 
  data={most_expensive_lanes} 
  x="valid_day"
  y="avg_price_usd"
  series="lane_description"
  title="Most Expensive Lanes in Date Range" 
/>

```sql country_codes
select
    country_code
from final.ports
group by country_code
```

<Dropdown data={country_codes} name=d_country_code value=country_code>
  <DropdownOption value="%" valueLabel="Select Destination Country Code"/>
</Dropdown>

## 5. FIND ALL LANES FOR A SPECIFIC COUNTRY (IMPORTS/EXPORTS)
-- Example: Find all imports into the United States.
```sql country_lanes
SELECT
    valid_day,
    origin_country_code, -- Country of export
    origin_port_code,
    equipment_id,
    avg_price_usd
FROM ${lane_prices_by_day}
WHERE
    destination_country_code like '${inputs.d_country_code.value}' -- Imports to the US
    AND dq_ok = TRUE
    AND valid_day BETWEEN '${inputs.range_filtering_a_query.start}' and '${inputs.range_filtering_a_query.end}'
ORDER BY avg_price_usd DESC;
```

<BarChart 
  data={country_lanes} 
  x="valid_day"
  y="origin_country_code"
  series="avg_price_usd"
  title="Origin Lanes for Country Code {inputs.d_country_code.value}" 
/>
