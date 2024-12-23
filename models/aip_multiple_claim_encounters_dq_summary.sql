
with multiple_claim_encounters as (
select
  patient_id,
  encounter_id,
  dq_problem,
  multiple_ms_drg_code,
  multiple_apr_drg_code,
  multiple_diagnosis_code_1,
  multiple_admit_type_code,
  multiple_admit_source_code,
  multiple_discharge_disposition_code,
  multiple_facility_npi,
  multiple_rendering_npi
from {{ ref('aip_multiple_claim_encounter_dq_flags') }}
),

count_of_multiple_claim_encounters as (
select count(*)
from multiple_claim_encounters
)


select
'Encounters with a DQ problem' as field,
(select count(*)
 from multiple_claim_encounters
 where dq_problem = 1
) as encounters,
round(
(select count(*)
 from multiple_claim_encounters
 where dq_problem = 1
) * 100.0 /
(select
  case
    when (select * from count_of_multiple_claim_encounters) = 0 then 1
    else (select * from count_of_multiple_claim_encounters)
  end 
) , 1) as percent_of_encounters


union all


select
'Encounters with a multiple MS-DRG' as field,
(select count(*)
 from multiple_claim_encounters
 where multiple_ms_drg_code = 1
) as encounters,
round(
(select count(*)
 from multiple_claim_encounters
 where multiple_ms_drg_code = 1
) * 100.0 /
(select
  case
    when (select * from count_of_multiple_claim_encounters) = 0 then 1
    else (select * from count_of_multiple_claim_encounters)
  end 
) , 1) as percent_of_encounters



union all


select
'Encounters with a multiple APR-DRG' as field,
(select count(*)
 from multiple_claim_encounters
 where multiple_apr_drg_code = 1
) as encounters,
round(
(select count(*)
 from multiple_claim_encounters
 where multiple_apr_drg_code = 1
) * 100.0 /
(select
  case
    when (select * from count_of_multiple_claim_encounters) = 0 then 1
    else (select * from count_of_multiple_claim_encounters)
  end 
) , 1) as percent_of_encounters



union all


select
'Encounters with a multiple Dx1' as field,
(select count(*)
 from multiple_claim_encounters
 where multiple_diagnosis_code_1 = 1
) as encounters,
round(
(select count(*)
 from multiple_claim_encounters
 where multiple_diagnosis_code_1 = 1
) * 100.0 /
(select
  case
    when (select * from count_of_multiple_claim_encounters) = 0 then 1
    else (select * from count_of_multiple_claim_encounters)
  end 
) , 1) as percent_of_encounters



union all


select
'Encounters with a multiple ATC' as field,
(select count(*)
 from multiple_claim_encounters
 where multiple_admit_type_code = 1
) as encounters,
round(
(select count(*)
 from multiple_claim_encounters
 where multiple_admit_type_code = 1
) * 100.0 /
(select
  case
    when (select * from count_of_multiple_claim_encounters) = 0 then 1
    else (select * from count_of_multiple_claim_encounters)
  end 
) , 1) as percent_of_encounters



union all


select
'Encounters with a multiple ASC' as field,
(select count(*)
 from multiple_claim_encounters
 where multiple_admit_source_code = 1
) as encounters,
round(
(select count(*)
 from multiple_claim_encounters
 where multiple_admit_source_code = 1
) * 100.0 /
(select
  case
    when (select * from count_of_multiple_claim_encounters) = 0 then 1
    else (select * from count_of_multiple_claim_encounters)
  end 
) , 1) as percent_of_encounters



union all


select
'Encounters with a multiple DDC' as field,
(select count(*)
 from multiple_claim_encounters
 where multiple_discharge_disposition_code = 1
) as encounters,
round(
(select count(*)
 from multiple_claim_encounters
 where multiple_discharge_disposition_code = 1
) * 100.0 /
(select
  case
    when (select * from count_of_multiple_claim_encounters) = 0 then 1
    else (select * from count_of_multiple_claim_encounters)
  end 
) , 1) as percent_of_encounters



union all


select
'Encounters with a multiple facility NPI' as field,
(select count(*)
 from multiple_claim_encounters
 where multiple_facility_npi = 1
) as encounters,
round(
(select count(*)
 from multiple_claim_encounters
 where multiple_facility_npi = 1
) * 100.0 /
(select
  case
    when (select * from count_of_multiple_claim_encounters) = 0 then 1
    else (select * from count_of_multiple_claim_encounters)
  end 
) , 1) as percent_of_encounters



union all


select
'Encounters with a multiple rendering NPI' as field,
(select count(*)
 from multiple_claim_encounters
 where multiple_rendering_npi = 1
) as encounters,
round(
(select count(*)
 from multiple_claim_encounters
 where multiple_rendering_npi = 1
) * 100.0 /
(select
  case
    when (select * from count_of_multiple_claim_encounters) = 0 then 1
    else (select * from count_of_multiple_claim_encounters)
  end 
) , 1) as percent_of_encounters
