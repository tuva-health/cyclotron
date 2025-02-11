
with eligibility_rows as (
select
  ingest_datetime,
  count(*) as eligibility_row_count
from {{ ref('eligibility') }}
group by ingest_datetime
),

eligibility_distinct_pk as (
select
  ingest_datetime,
  count(distinct patient_id, plan, enrollment_start_date, enrollment_end_date) as
  eligibility_pk_count
from {{ ref('eligibility') }}
group by ingest_datetime
),

join_both as (
select
  aa.ingest_datetime,
  aa.eligibility_row_count,
  bb.eligibility_pk_count,
  case
    when (aa.eligibility_row_count = bb.eligibility_pk_count) then 'YES'
    else 'NO'
  end as correct_pk
from eligibility_rows aa
left join eligibility_distinct_pk bb
on (aa.ingest_datetime = bb.ingest_datetime) or
(aa.ingest_datetime is null and bb.ingest_datetime is null)
)
	


select *
from join_both
