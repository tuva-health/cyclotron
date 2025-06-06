


with professional_paid_amounts as (
select
  encounter_id,
  sum(paid_amount) as paid_amount
from {{ ref('prof_claims_overlapping_with_one_encounter') }}
group by encounter_id
)


select
  aa.patient_id,
  aa.encounter_id,
  aa.encounter_start_date,
  aa.encounter_end_date,
  (aa.encounter_end_date - aa.encounter_start_date) as los,
  aa.ms_drg_code,
  aa.apr_drg_code,
  aa.diagnosis_code_1,
  aa.admit_type_code,
  aa.admit_source_code,
  aa.discharge_disposition_code,
  aa.facility_npi,
  aa.rendering_npi,
  aa.paid_amount as institutional_paid_amount,
  aa.dq_problem,
  aa.usable_ms_drg_code,
  aa.usable_apr_drg_code,
  aa.usable_diagnosis_code_1,
  aa.usable_admit_type_code,
  aa.usable_admit_source_code,
  aa.usable_discharge_disposition_code,
  aa.usable_facility_npi,
  aa.usable_rendering_npi,
  aa.single_claim_encounter,
  aa.multi_claim_encounter,

  case
    when bb.encounter_id is not null then 1
    else 0
  end as has_professional_claims,

  bb.paid_amount as professional_paid_amount,

  (aa.paid_amount + bb.paid_amount) as total_paid_amount
  

from {{ ref('aip_encounters_institutional_definition') }} aa
left join professional_paid_amounts bb
on aa.encounter_id = bb.encounter_id
