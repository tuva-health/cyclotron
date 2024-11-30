

with claims_per_encounter as (
select
  patient_id,
  encounter_id,
  count(distinct claim_id) as claims
from {{ ref('aip_multiple_claim_encounters') }}
where usable_for_aip_encounter = 1
group by patient_id, encounter_id
),

summary as (
select
  claims as number_of_claims_in_encounter,
  count(*) number_of_times_this_happens
from claims_per_encounter
group by claims
)


select *
from summary

