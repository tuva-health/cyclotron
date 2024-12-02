

with number_of_encounters_each_prof_claim_overlaps_with as (
select
  claim_id,
  patient_id,
  count(distinct encounter_id) as encounters_claim_overlaps_with
from {{ ref('prof_claims_overlapping_with_aip_encounters') }}
group by claim_id, patient_id
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
(select * from claims_overlapping_with_one_encounter) as field_value

union all

select
'Prof claims overlapping with multiple encounters' as field,
(select * from claims_overlapping_with_multiple_encounters) as field_value

union all

select
'Prof claims overlapping with no encounters' as field,
(select * from claims_overlapping_with_no_encounters) as field_value
