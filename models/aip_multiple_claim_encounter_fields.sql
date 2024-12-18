

with add_row_number_to_claims as (
select
  *,
  
  row_number() over (partition by patient_id, encounter_id
                     order by merge_start_date, merge_end_date, claim_id) as row_num_first,
		     
  row_number() over (partition by patient_id, encounter_id
                     order by merge_end_date, merge_start_date, claim_id desc) as row_num_last

from {{ ref('aip_multiple_claim_encounters') }}
where usable_for_aip_encounter = 1
),

encounter_paid_amount as (
select
  patient_id,
  encounter_id,
  sum(paid_amount) as paid_amount
from add_row_number_to_claims
group by patient_id, encounter_id
),

encounter_ms_drg_code_row as (
select
  patient_id,
  encounter_id,
  min(row_num_first) as ms_drg_code_row
from add_row_number_to_claims
where ms_drg_code is not null
group by patient_id, encounter_id
),

encounter_ms_drg_code as (
select
  aa.patient_id,
  aa.encounter_id,
  aa.ms_drg_code
from add_row_number_to_claims aa
inner join encounter_ms_drg_code_row bb
on aa.patient_id = bb.patient_id
and aa.encounter_id = bb.encounter_id
and aa.row_num_first = bb.ms_drg_code_row
),


encounter_apr_drg_code_row as (
select
  patient_id,
  encounter_id,
  min(row_num_first) as apr_drg_code_row
from add_row_number_to_claims
where apr_drg_code is not null
group by patient_id, encounter_id
),

encounter_apr_drg_code as (
select
  aa.patient_id,
  aa.encounter_id,
  aa.apr_drg_code
from add_row_number_to_claims aa
inner join encounter_apr_drg_code_row bb
on aa.patient_id = bb.patient_id
and aa.encounter_id = bb.encounter_id
and aa.row_num_first = bb.apr_drg_code_row
),


encounter_diagnosis_code_1_row as (
select
  patient_id,
  encounter_id,
  min(row_num_first) as diagnosis_code_1_row
from add_row_number_to_claims
where diagnosis_code_1 is not null
group by patient_id, encounter_id
),

encounter_diagnosis_code_1 as (
select
  aa.patient_id,
  aa.encounter_id,
  aa.diagnosis_code_1
from add_row_number_to_claims aa
inner join encounter_diagnosis_code_1_row bb
on aa.patient_id = bb.patient_id
and aa.encounter_id = bb.encounter_id
and aa.row_num_first = bb.diagnosis_code_1_row
),


encounter_admit_type_code_row as (
select
  patient_id,
  encounter_id,
  min(row_num_first) as admit_type_code_row
from add_row_number_to_claims
where admit_type_code is not null
group by patient_id, encounter_id
),

encounter_admit_type_code as (
select
  aa.patient_id,
  aa.encounter_id,
  aa.admit_type_code
from add_row_number_to_claims aa
inner join encounter_admit_type_code_row bb
on aa.patient_id = bb.patient_id
and aa.encounter_id = bb.encounter_id
and aa.row_num_first = bb.admit_type_code_row
),


encounter_admit_source_code_row as (
select
  patient_id,
  encounter_id,
  min(row_num_first) as admit_source_code_row
from add_row_number_to_claims
where admit_source_code is not null
group by patient_id, encounter_id
),

encounter_admit_source_code as (
select
  aa.patient_id,
  aa.encounter_id,
  aa.admit_source_code
from add_row_number_to_claims aa
inner join encounter_admit_source_code_row bb
on aa.patient_id = bb.patient_id
and aa.encounter_id = bb.encounter_id
and aa.row_num_first = bb.admit_source_code_row
),


encounter_discharge_disposition_code_row as (
select
  patient_id,
  encounter_id,
  min(row_num_last) as discharge_disposition_code_row
from add_row_number_to_claims
where discharge_disposition_code is not null
group by patient_id, encounter_id
),

encounter_discharge_disposition_code as (
select
  aa.patient_id,
  aa.encounter_id,
  aa.discharge_disposition_code
from add_row_number_to_claims aa
inner join encounter_discharge_disposition_code_row bb
on aa.patient_id = bb.patient_id
and aa.encounter_id = bb.encounter_id
and aa.row_num_last = bb.discharge_disposition_code_row
),


encounter_facility_npi_row as (
select
  patient_id,
  encounter_id,
  min(row_num_first) as facility_npi_row
from add_row_number_to_claims
where facility_npi is not null
group by patient_id, encounter_id
),

encounter_facility_npi as (
select
  aa.patient_id,
  aa.encounter_id,
  aa.facility_npi
from add_row_number_to_claims aa
inner join encounter_facility_npi_row bb
on aa.patient_id = bb.patient_id
and aa.encounter_id = bb.encounter_id
and aa.row_num_first = bb.facility_npi_row
),


encounter_rendering_npi_row as (
select
  patient_id,
  encounter_id,
  min(row_num_first) as rendering_npi_row
from add_row_number_to_claims
where rendering_npi is not null
group by patient_id, encounter_id
),

encounter_rendering_npi as (
select
  aa.patient_id,
  aa.encounter_id,
  aa.rendering_npi
from add_row_number_to_claims aa
inner join encounter_rendering_npi_row bb
on aa.patient_id = bb.patient_id
and aa.encounter_id = bb.encounter_id
and aa.row_num_first = bb.rendering_npi_row
),


encounter_start_and_end_dates_and_dq_flags as (
select
  patient_id,
  encounter_id,
  min(merge_start_date) as encounter_start_date,
  max(merge_end_date) as encounter_end_date,
  max(dq_problem) as dq_problem,
  max(usable_patient_id) as usable_patient_id,
  max(usable_merge_dates) as usable_merge_dates,
  max(usable_ms_drg_code) as usable_ms_drg_code,
  max(usable_apr_drg_code) as usable_apr_drg_code,
  max(usable_diagnosis_code_1) as usable_diagnosis_code_1,
  max(usable_admit_type_code) as usable_admit_type_code,
  max(usable_admit_source_code) as usable_admit_source_code,
  max(usable_discharge_disposition_code) as usable_discharge_disposition_code,
  max(usable_facility_npi) as usable_facility_npi,
  max(usable_rendering_npi) as usable_rendering_npi
from add_row_number_to_claims
group by patient_id, encounter_id
),


all_encounters as (
select distinct patient_id, encounter_id
from add_row_number_to_claims
)


select
  enc.patient_id,
  enc.encounter_id,

  dates_and_flags.encounter_start_date,
  dates_and_flags.encounter_end_date,

  ms_drg_code.ms_drg_code,
  apr_drg_code.apr_drg_code,
  diagnosis_code_1.diagnosis_code_1,
  admit_type_code.admit_type_code,
  admit_source_code.admit_source_code,
  discharge_disposition_code.discharge_disposition_code,
  facility_npi.facility_npi,
  rendering_npi.rendering_npi,
  encounter_paid_amount.paid_amount,

  dates_and_flags.dq_problem,
  dates_and_flags.usable_ms_drg_code,
  dates_and_flags.usable_apr_drg_code,
  dates_and_flags.usable_diagnosis_code_1,
  dates_and_flags.usable_admit_type_code,
  dates_and_flags.usable_admit_source_code,
  dates_and_flags.usable_discharge_disposition_code,
  dates_and_flags.usable_facility_npi,
  dates_and_flags.usable_rendering_npi

from all_encounters enc

left join encounter_ms_drg_code ms_drg_code
on enc.patient_id = ms_drg_code.patient_id
and enc.encounter_id = ms_drg_code.encounter_id

left join encounter_apr_drg_code apr_drg_code
on enc.patient_id = apr_drg_code.patient_id
and enc.encounter_id = apr_drg_code.encounter_id

left join encounter_diagnosis_code_1 diagnosis_code_1
on enc.patient_id = diagnosis_code_1.patient_id
and enc.encounter_id = diagnosis_code_1.encounter_id

left join encounter_admit_type_code admit_type_code
on enc.patient_id = admit_type_code.patient_id
and enc.encounter_id = admit_type_code.encounter_id

left join encounter_admit_source_code admit_source_code
on enc.patient_id = admit_source_code.patient_id
and enc.encounter_id = admit_source_code.encounter_id

left join encounter_discharge_disposition_code discharge_disposition_code
on enc.patient_id = discharge_disposition_code.patient_id
and enc.encounter_id = discharge_disposition_code.encounter_id

left join encounter_facility_npi facility_npi
on enc.patient_id = facility_npi.patient_id
and enc.encounter_id = facility_npi.encounter_id

left join encounter_rendering_npi rendering_npi
on enc.patient_id = rendering_npi.patient_id
and enc.encounter_id = rendering_npi.encounter_id

left join encounter_start_and_end_dates_and_dq_flags dates_and_flags
on enc.patient_id = dates_and_flags.patient_id
and enc.encounter_id = dates_and_flags.encounter_id

left join encounter_paid_amount
on enc.patient_id = encounter_paid_amount.patient_id
and enc.encounter_id = encounter_paid_amount.encounter_id
