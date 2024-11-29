



with all_aip_inst_claims as (
select claim_id
from {{ ref('aip_venn_diagram') }}
where rb_drg_bill = 1
),


patient_id_facility_npi_and_rendering_npi_for_all_claims as (
select
  claim_id,
  max(patient_id) as patient_id,
  max(facility_npi) as facility_npi,
  max(rendering_npi) as rendering_npi
from {{ ref('medical_claim') }}
group by claim_id
),


claims_with_usable_patient_id as (
select claim_id
from (
     select
       claim_id,
       max(patient_id) as max_patient_id,
       min(patient_id) as min_patient_id
     from {{ ref('medical_claim') }}
     where patient_id is not null
     group by claim_id
     having max_patient_id = min_patient_id
)    
),


claims_with_usable_facility_npi as (
select claim_id
from (
     select
       claim_id,
       max(facility_npi) as max_facility_npi,
       min(facility_npi) as min_facility_npi
     from {{ ref('medical_claim') }}
     where facility_npi is not null
     group by claim_id
     having max_facility_npi = min_facility_npi
)     
),


claims_with_usable_rendering_npi as (
select claim_id
from (
     select
       claim_id,
       max(rendering_npi) as max_rendering_npi,
       min(rendering_npi) as min_rendering_npi
     from {{ ref('medical_claim') }}
     where rendering_npi is not null
     group by claim_id
     having max_rendering_npi = min_rendering_npi
)     
),


add_all_final_columns as (
select
  aip.claim_id,
  
  other_fields.patient_id,
  
  merge_dates.merge_start_date,
  merge_dates.merge_end_date,

  header_values.assigned_ms_drg_code as ms_drg_code,
  header_values.assigned_apr_drg_code as apr_drg_code,
  header_values.assigned_diagnosis_code_1 as diagnosis_code_1,
  header_values.assigned_admit_type_code as admit_type_code,
  header_values.assigned_admit_source_code as admit_source_code,
  header_values.assigned_discharge_disposition_code as discharge_disposition_code,

  other_fields.facility_npi,
  other_fields.rendering_npi,

  case
    when usable_patient_id.claim_id is not null then 1
    else 0
  end as usable_patient_id,

  merge_dates.usable_merge_dates,

  header_values.usable_diagnosis_code_1,
  header_values.usable_admit_type_code,
  header_values.usable_admit_source_code,
  header_values.usable_discharge_disposition_code,

  case
    when usable_facility_npi.claim_id is not null then 1
    else 0
  end as usable_facility_npi,

  case
    when usable_rendering_npi.claim_id is not null then 1
    else 0
  end as usable_rendering_npi

 
from all_aip_inst_claims aip

left join {{ ref('header_values') }} header_values
on aip.claim_id = header_values.claim_id

left join {{ ref('claim_grain_calculated_dates') }} merge_dates
on aip.claim_id = merge_dates.claim_id

left join claims_with_usable_patient_id usable_patient_id
on aip.claim_id = usable_patient_id.claim_id

left join claims_with_usable_facility_npi usable_facility_npi
on aip.claim_id = usable_facility_npi.claim_id

left join claims_with_usable_rendering_npi usable_rendering_npi
on aip.claim_id = usable_rendering_npi.claim_id

left join patient_id_facility_npi_and_rendering_npi_for_all_claims other_fields
on aip.claim_id = other_fields.claim_id
),



add_dq_problem_and_dq_insight_flags as (
select
  claim_id,
  patient_id,
  merge_start_date,
  merge_end_date,
  ms_drg_code,
  apr_drg_code,
  diagnosis_code_1,
  admit_type_code,
  admit_source_code,
  discharge_disposition_code,
  facility_npi,
  rendering_npi,

  case
    when  ( (usable_patient_id = 1) and
            (usable_merge_dates = 1) 
	  ) then 1
    else 0
  end as usable_for_aip_encounter,

  case
    when  ( (usable_patient_id = 0) or
            (usable_merge_dates = 0) or
            (usable_diagnosis_code_1 = 0) or
            (usable_admit_type_code = 0) or
            (usable_admit_source_code = 0) or
            (usable_discharge_disposition_code = 0) or	    
            (usable_rendering_npi = 0) or
            (usable_facility_npi = 0)
	  ) then 1
    else 0
  end as dq_problem,

  usable_patient_id,
  usable_merge_dates,
  usable_diagnosis_code_1,
  usable_admit_type_code,
  usable_admit_source_code,
  usable_discharge_disposition_code,
  usable_facility_npi,
  usable_rendering_npi
  
from add_all_final_columns
)


select *
from add_dq_problem_and_dq_insight_flags
