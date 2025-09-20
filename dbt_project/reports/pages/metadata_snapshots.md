---
title: Metadata Coverage Snapshots
---

# Metadata Coverage Snapshots

This page provides visibility into how data coverage is developing over time as new batches of shipping contract data are ingested.  
The snapshots are generated using the `snapshot_coverage` macro, capturing the number of distinct (origin, destination, equipment type, day) combinations with `dq_ok = TRUE` **before** and **after** each data update.

```sql coverage_snapshots
select
    snapshot_ts,
    stage,
    covered_lane_count
from final.metadata_coverage
order by snapshot_ts desc
```

## Snapshot Trends Over Time

<LineChart
  data={coverage_snapshots}
  x="snapshot_ts"
  y="covered_lane_count"
  title="Coverage Trends"
  subtitle="Number of covered lanes captured at each snapshot"
/>

## Snapshot Data Table

<DataTable
  data={coverage_snapshots}
  title="Metadata Coverage Snapshots"
  columns={{
    snapshot_ts: { type: 'timestamp' },
    stage: { type: 'text' },
    covered_lane_count: { type: 'number' }
  }}
  rowsPerPage={10}
/>
