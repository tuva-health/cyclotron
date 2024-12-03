

with total_aip_prof_claims as (
select count(*)
from {{ ref('all_prof_aip_claims') }}
),

aip_prof_claims_with_unusable_patient_id as (
select count(*)
from {{ ref('all_prof_aip_claims') }}
where usable_patient_id = 0
),

aip_prof_claims_with_unusable_merge_dates as (
select count(*)
from {{ ref('all_prof_aip_claims') }}
where usable_merge_dates = 0
),

usable_aip_prof_claims as (
select count(*)
from {{ ref('all_prof_aip_claims') }}
where usable_prof_claim = 1
)


select
'total aip prof claims' as field,
(select * from total_aip_prof_claims) as field_value

union all

select
'(aip prof claims with unusable patient_id) / (total aip prof claims) * 100' as field,
round((select * from aip_prof_claims_with_unusable_patient_id) * 100.0 /
(select * from total_aip_prof_claims),1) as field_value

union all

select
'(aip prof claims with unusable merge dates) / (total aip prof claims) * 100' as field,
round((select * from aip_prof_claims_with_unusable_merge_dates) * 100.0 /
(select * from total_aip_prof_claims),1) as field_value

union all

select
'(usable aip prof claims) / (total aip prof claims) * 100' as field,
round((select * from usable_aip_prof_claims) * 100.0 /
(select * from total_aip_prof_claims),1) as field_value

