---
title: Data Quality Coverage
---

# Data Quality Insights

This dashboard tracks **data quality coverage** for daily lane-equipment combinations.  
We apply Xeneta's rule:  
- ✅ **dq_ok = TRUE** → at least **5 distinct companies** and **2 distinct suppliers**  
- ❌ **dq_ok = FALSE** → insufficient coverage  

---

## Coverage Overview

```sql dq_summary
select
    valid_day,
    count(*) filter (where dq_ok = true) as covered_lanes,
    count(*) filter (where dq_ok = false) as uncovered_lanes
from final.daily_lane_equipment_prices
group by valid_day
order by valid_day
```

<LineChart 
  data={dq_summary} 
  x="valid_day" 
  y={["covered_lanes","uncovered_lanes"]}
  title="Covered vs Uncovered Lanes Over Time"
  subtitle="Daily count of lane-equipment combinations"
/>

---

## Lane Failures

```sql dq_failures
select
    valid_day,
    equipment_id,
    origin_port_name,
    destination_port_name,
    distinct_companies_count,
    distinct_suppliers_count
from final.daily_lane_equipment_prices
where dq_ok = false
order by valid_day, equipment_id
```

<DataTable 
  data={dq_failures} 
  title="Lanes Failing Coverage Requirement" 
  columns={{
    valid_day: { type: 'date' },
    equipment_id: { type: 'number' },
    origin_port_name: { type: 'text' },
    destination_port_name: { type: 'text' },
    distinct_companies_count: { type: 'number' },
    distinct_suppliers_count: { type: 'number' }
  }} 
  rowsPerPage={10}
/>

```sql equipment_types
select
    EQUIPMENT_ID
from final.equipment_type
group by EQUIPMENT_ID
order by EQUIPMENT_ID
```

<Dropdown data={equipment_types} name=equipment_type value=equipment_id>
  <DropdownOption value=null valueLabel="Select Equipment"/>
</Dropdown>

<DateInput
    name=date_filtered_prices
    data={lane_prices_by_day}
    dates=valid_day
    start="2022-01-01"
    end="2022-06-01"
    valueLabel="Select Date"
/>

## Data Coverage Before/After a Data Load
-- Monitor how new data improves visibility.
-- Run this before and after a new `charges_N`/`datapoints_N` load.
```sql data_coverage
SELECT
    valid_day,
    COUNT(*) AS total_lane_days,
    COUNT(CASE WHEN dq_ok THEN 1 END) AS reliable_lane_days,
    ROUND( (COUNT(CASE WHEN dq_ok THEN 1 END) * 100.0 / COUNT(*)), 2) AS pct_reliable
FROM ${lane_prices_by_day}
WHERE equipment_id = coalesce(${inputs.equipment_type.value}, equipment_id)
GROUP BY valid_day
ORDER BY valid_day;
```

## Data Quality Failures
-- Understand *why* a specific lane isn't meeting DQ standards.
```sql dq_failures_investigation
SELECT
    valid_day,
    origin_port_code,
    destination_port_code,
    equipment_id,
    -- Flag the specific reason for failure:
    CASE
        WHEN distinct_companies_count < 5 THEN 'Needs more companies'
        WHEN distinct_suppliers_count < 2 THEN 'Needs more suppliers'
        ELSE 'OK'
    END AS dq_issue
FROM ${lane_prices_by_day}
WHERE
    dq_ok = FALSE
    AND valid_day > '${inputs.date_filtered_prices.value}'
ORDER BY valid_day, distinct_companies_count DESC;
```