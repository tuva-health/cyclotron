

with all_prof_aip_claim_lines as (
select
  claim_id,
  patient_id,
  place_of_service_code,
  paid_amount
from {{ ref('medical_claim') }}
where place_of_service_code = '21'
),


group_at_claim_grain as (
select
  claim_id,
  max(patient_id) as patient_id,
  sum(paid_amount) as paid_amount
from all_prof_aip_claim_lines
group by claim_id
),


add_other_fields as (
select
  aa.claim_id,
  aa.patient_id,
  aa.paid_amount,

  bb.usable_patient_id,

  cc.merge_start_date,
  cc.merge_end_date,
  cc.usable_merge_dates
  
from group_at_claim_grain aa

left join {{ ref('other_header_values') }} bb
on aa.claim_id = bb.claim_id
and aa.patient_id = bb.patient_id

left join {{ ref('claim_grain_calculated_dates') }} cc
on aa.claim_id = cc.claim_id
)


select *
from add_other_fields
