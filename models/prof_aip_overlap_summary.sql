

with number_of_encounters_each_prof_claim_overlaps_with as (
select
  claim_id,
  patient_id,
  count(distinct encounter_id) as encounters_claim_overlaps_with
from {{ ref('prof_claims_overlapping_with_aip_encounters') }}
group by claim_id, patient_id
),

total_usable_aip_professional_claims as (
select count(distinct claim_id)
from {{ ref('prof_claims_overlapping_with_aip_encounters') }}
),


claims_overlapping_with_one_encounter as (
select count(distinct claim_id)
from number_of_encounters_each_prof_claim_overlaps_with
where encounters_claim_overlaps_with = 1
),

claims_overlapping_with_multiple_encounters as (
select count(distinct claim_id)
from number_of_encounters_each_prof_claim_overlaps_with
where encounters_claim_overlaps_with > 1
),

claims_overlapping_with_no_encounters as (
select count(distinct claim_id)
from number_of_encounters_each_prof_claim_overlaps_with
where encounters_claim_overlaps_with = 0
)





select
'Prof claims overlapping with one encounter' as field,
(select * from claims_overlapping_with_one_encounter) as number_of_claims,
round( (select * from claims_overlapping_with_one_encounter) * 100.0 /
       (select * from total_usable_aip_professional_claims)
       , 1) as percent_of_usable_aip_prof_claims

union all

select
'Prof claims overlapping with multiple encounters' as field,
(select * from claims_overlapping_with_multiple_encounters) as number_of_claims,
round( (select * from claims_overlapping_with_multiple_encounters) * 100.0 /
       (select * from total_usable_aip_professional_claims)
       , 1) as percent_of_usable_aip_prof_claims


union all

select
'Prof claims overlapping with no encounters' as field,
(select * from claims_overlapping_with_no_encounters) as number_of_claims,
round( (select * from claims_overlapping_with_no_encounters) * 100.0 /
       (select * from total_usable_aip_professional_claims)
       , 1) as percent_of_usable_aip_prof_claims

