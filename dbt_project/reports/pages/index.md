---
title: Daily Shipping Price Aggregations
---

# Daily Shipping Price Insights

Welcome to the daily shipping price aggregation dashboard. This page presents the average and median daily prices for ocean shipping contracts, categorized by equipment type and transportation lane. Data shown here has passed our rigorous data quality checks, ensuring "sufficient coverage".

```sql equipment_types
select
    equipment_id
from final.equipment_type
group by equipment_id
```

<Dropdown data={equipment_types} name=equipment_type value=equipment_id>
  <DropdownOption value=null valueLabel="Select Equipment"/>
</Dropdown>

The `daily_shipping_prices` query pulls data from our `final` schema.

```sql daily_shipping_prices
select
    valid_day,
    equipment_id,
    origin_region_name,
    destination_region_name,
    origin_port_name,
    destination_port_name,
    avg_price_usd,
    median_price_usd,
    dq_ok,
    concat(origin_port_name, ' to ', destination_port_name) as lane_description
from final.daily_lane_equipment_prices
where dq_ok = TRUE
and equipment_id = coalesce(${inputs.equipment_type.value}, equipment_id)
order by valid_day, equipment_id, origin_region_name, destination_region_name
```

## Key Metrics
Here's an overview of the most recent reliable data available:
Latest Available Date
The latest day for which we have quality-checked aggregated price data is: <Value data={daily_shipping_prices} column=valid_day />.
Price Trends by Equipment Type
Let's visualize the average daily shipping prices for different equipment types across all reliable lanes.

<LineChart
  data={daily_shipping_prices}
  x="valid_day"
  y="avg_price_usd"
  series="equipment_id"
  title="Average Daily Price by Equipment Type (USD)"
  subtitle="Across all lanes with sufficient data coverage"
/>

<BarChart 
  data={daily_shipping_prices} 
  x="lane_description" 
  y="avg_price_usd" 
  series="equipment_id"
  title="Average Price by Lane (USD) by Equipment Type" 
  subtitle="Filtered for equipment_id = {inputs.equipment_type.value} and dq_ok = TRUE" 
/>

<BarChart 
  data={daily_shipping_prices} 
  x="lane_description" 
  y="median_price_usd" 
  series="equipment_id"
  title="Median Price by Lane (USD) by Equipment Type" 
  subtitle="Filtered for equipment_id = {inputs.equipment_type.value} and dq_ok = TRUE" 
/>

<DataTable 
  data={daily_shipping_prices} 
  title="Daily Aggregated Shipping Prices (Quality-Checked)" 
  columns={{ 
    valid_day: { type: 'date' }, 
    equipment_id: { type: 'number' }, 
    origin_region_name: { type: 'text' }, 
    destination_region_name: { type: 'text' }, 
    avg_price_usd: { type: 'number', format: 'usd' }, 
    median_price_usd: { type: 'number', format: 'usd' }, 
    dq_ok: { type: 'boolean' } 
  }} 
  rowsPerPage={10} 
/>
