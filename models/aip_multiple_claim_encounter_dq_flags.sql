

with add_flags_for_each_relevant_field as (
select
  patient_id,
  encounter_id,
  
  case when (count(distinct ms_drg_code) > 1) then 1
       else 0
  end as multiple_ms_drg_code,

  case when (count(distinct apr_drg_code) > 1) then 1
       else 0
  end as multiple_apr_drg_code,

  case when (count(distinct diagnosis_code_1) > 1) then 1
       else 0
  end as multiple_diagnosis_code_1,

  case when (count(distinct admit_type_code) > 1) then 1
       else 0
  end as multiple_admit_type_code,

  case when (count(distinct admit_source_code) > 1) then 1
       else 0
  end as multiple_admit_source_code,

  case when (count(distinct discharge_disposition_code) > 1) then 1
       else 0
  end as multiple_discharge_disposition_code,

  case when (count(distinct facility_npi) > 1) then 1
       else 0
  end as multiple_facility_npi,

  case when (count(distinct rendering_npi) > 1) then 1
       else 0
  end as multiple_rendering_npi

from {{ ref('aip_multiple_claim_encounters') }}
where usable_for_aip_encounter = 1
group by patient_id, encounter_id
),


add_dq_problem_flag as (
select
  patient_id,
  encounter_id,

  case
    when
        (multiple_ms_drg_code = 1 or
	 multiple_apr_drg_code = 1 or
	 multiple_diagnosis_code_1 = 1 or
	 multiple_admit_type_code = 1 or
	 multiple_admit_source_code = 1 or
	 multiple_discharge_disposition_code = 1 or
	 multiple_facility_npi = 1 or
	 multiple_rendering_npi = 1
	) then 1
    else 0
  end as dq_problem,

  multiple_ms_drg_code,
  multiple_apr_drg_code,
  multiple_diagnosis_code_1,
  multiple_admit_type_code,
  multiple_admit_source_code,
  multiple_discharge_disposition_code,
  multiple_facility_npi,
  multiple_rendering_npi

from add_flags_for_each_relevant_field
)



select *
from add_dq_problem_flag
