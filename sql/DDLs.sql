create or replace view final.metadata_coverage_changes as
with paired as (
    select
        b.snapshot_ts as before_ts,
        a.snapshot_ts as after_ts,
        b.covered_lane_count as before_count,
        a.covered_lane_count as after_count
    from final.metadata_coverage b
    join final.metadata_coverage a
      on a.stage = 'after'
     and b.stage = 'before'
     -- pair by closest before/after timestamps
     and a.snapshot_ts > b.snapshot_ts
    qualify row_number() over (partition by b.snapshot_ts order by a.snapshot_ts) = 1
)
select
    before_ts,
    after_ts,
    before_count,
    after_count,
    after_count - before_count as diff_count,
    case 
        when before_count is null or before_count = 0 then null
        else round(((after_count - before_count) * 100.0 / before_count), 2)
    end as pct_change
from paired
order by after_ts;
