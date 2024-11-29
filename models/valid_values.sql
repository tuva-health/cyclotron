


select
  mc.claim_id,
  mc.claim_line_number,
  
  mc.bill_type_code,
  case
    when bill_type.bill_type_code is not null then 1
    else 0
  end as valid_bill_type_code,

  mc.ms_drg_code,
  case
    when ms_drg.ms_drg_code is not null then 1
    else 0
  end as valid_ms_drg_code,
  
  mc.apr_drg_code,
  case
    when apr_drg.apr_drg_code is not null then 1
    else 0
  end as valid_apr_drg_code,

  mc.admit_type_code,
  case
    when admit_type.admit_type_code is not null then 1
    else 0
  end as valid_admit_type_code,

  mc.admit_source_code,
  case
    when admit_source.admit_source_code is not null then 1
    else 0
  end as valid_admit_source_code,
    
  mc.discharge_disposition_code,
  case
    when discharge_disposition.discharge_disposition_code is not null then 1
    else 0
  end as valid_discharge_disposition_code,
  
  mc.revenue_center_code,
  case
    when revenue_center.revenue_center_code is not null then 1
    else 0
  end as valid_revenue_center_code,
  
  mc.place_of_service_code,
  case
    when place_of_service.place_of_service_code is not null then 1
    else 0
  end as valid_place_of_service_code,
  
  mc.admission_date,
  case
    when admission.full_date is not null then 1
    else 0
  end as valid_admission_date,
  
  mc.discharge_date,
  case
    when discharge.full_date is not null then 1
    else 0
  end as valid_discharge_date,

  mc.claim_start_date,
  case
    when claim_start.full_date is not null then 1
    else 0
  end as valid_claim_start_date,

  mc.claim_end_date,
  case
    when claim_end.full_date is not null then 1
    else 0
  end as valid_claim_end_date,

  mc.claim_line_start_date,
  case
    when claim_line_start.full_date is not null then 1
    else 0
  end as valid_claim_line_start_date,

  mc.claim_line_end_date,
  case
    when claim_line_end.full_date is not null then 1
    else 0
  end as valid_claim_line_end_date,

  mc.diagnosis_code_1,
  case
    when icd1.icd_10_cm is not null then 1
    else 0
  end as valid_diagnosis_code_1,

  mc.diagnosis_code_2,
  case
    when icd2.icd_10_cm is not null then 1
    else 0
  end as valid_diagnosis_code_2,

  mc.diagnosis_code_3,
  case
    when icd3.icd_10_cm is not null then 1
    else 0
  end as valid_diagnosis_code_3,

  mc.diagnosis_code_4,
  case
    when icd4.icd_10_cm is not null then 1
    else 0
  end as valid_diagnosis_code_4,

  mc.diagnosis_code_5,
  case
    when icd5.icd_10_cm is not null then 1
    else 0
  end as valid_diagnosis_code_5,

  mc.diagnosis_code_6,
  case
    when icd6.icd_10_cm is not null then 1
    else 0
  end as valid_diagnosis_code_6,

  mc.diagnosis_code_7,
  case
    when icd7.icd_10_cm is not null then 1
    else 0
  end as valid_diagnosis_code_7,

  mc.diagnosis_code_8,
  case
    when icd8.icd_10_cm is not null then 1
    else 0
  end as valid_diagnosis_code_8,

  mc.diagnosis_code_9,
  case
    when icd9.icd_10_cm is not null then 1
    else 0
  end as valid_diagnosis_code_9,

  mc.diagnosis_code_10,
  case
    when icd10.icd_10_cm is not null then 1
    else 0
  end as valid_diagnosis_code_10,

  mc.diagnosis_code_11,
  case
    when icd11.icd_10_cm is not null then 1
    else 0
  end as valid_diagnosis_code_11,

  mc.diagnosis_code_12,
  case
    when icd12.icd_10_cm is not null then 1
    else 0
  end as valid_diagnosis_code_12,

  mc.diagnosis_code_13,
  case
    when icd13.icd_10_cm is not null then 1
    else 0
  end as valid_diagnosis_code_13,

  mc.diagnosis_code_14,
  case
    when icd14.icd_10_cm is not null then 1
    else 0
  end as valid_diagnosis_code_14,

  mc.diagnosis_code_15,
  case
    when icd15.icd_10_cm is not null then 1
    else 0
  end as valid_diagnosis_code_15,

  mc.diagnosis_code_16,
  case
    when icd16.icd_10_cm is not null then 1
    else 0
  end as valid_diagnosis_code_16,

  mc.diagnosis_code_17,
  case
    when icd17.icd_10_cm is not null then 1
    else 0
  end as valid_diagnosis_code_17,

  mc.diagnosis_code_18,
  case
    when icd18.icd_10_cm is not null then 1
    else 0
  end as valid_diagnosis_code_18,

  mc.diagnosis_code_19,
  case
    when icd19.icd_10_cm is not null then 1
    else 0
  end as valid_diagnosis_code_19,

  mc.diagnosis_code_20,
  case
    when icd20.icd_10_cm is not null then 1
    else 0
  end as valid_diagnosis_code_20,

  mc.diagnosis_code_21,
  case
    when icd21.icd_10_cm is not null then 1
    else 0
  end as valid_diagnosis_code_21,

  mc.diagnosis_code_22,
  case
    when icd22.icd_10_cm is not null then 1
    else 0
  end as valid_diagnosis_code_22,

  mc.diagnosis_code_23,
  case
    when icd23.icd_10_cm is not null then 1
    else 0
  end as valid_diagnosis_code_23,

  mc.diagnosis_code_24,
  case
    when icd24.icd_10_cm is not null then 1
    else 0
  end as valid_diagnosis_code_24,

  mc.diagnosis_code_25,
  case
    when icd25.icd_10_cm is not null then 1
    else 0
  end as valid_diagnosis_code_25



from {{ ref('medical_claim') }} mc

left join {{ ref('terminology__bill_type') }} bill_type
on mc.bill_type_code = bill_type.bill_type_code

left join {{ ref('terminology__ms_drg') }} ms_drg
on mc.ms_drg_code = ms_drg.ms_drg_code

left join {{ ref('terminology__apr_drg') }} apr_drg
on mc.apr_drg_code = apr_drg.apr_drg_code

left join {{ ref('terminology__admit_type') }} admit_type
on mc.admit_type_code = admit_type.admit_type_code

left join {{ ref('terminology__admit_source') }} admit_source
on mc.admit_source_code = admit_source.admit_source_code

left join {{ ref('terminology__discharge_disposition') }} discharge_disposition
on mc.discharge_disposition_code = discharge_disposition.discharge_disposition_code

left join {{ ref('terminology__revenue_center') }} revenue_center
on mc.revenue_center_code = revenue_center.revenue_center_code

left join {{ ref('terminology__place_of_service') }} place_of_service
on mc.place_of_service_code = place_of_service.place_of_service_code

left join {{ ref('reference_data__calendar') }} admission
on mc.admission_date = admission.full_date

left join {{ ref('reference_data__calendar') }} discharge
on mc.discharge_date = discharge.full_date

left join {{ ref('reference_data__calendar') }} claim_start
on mc.claim_start_date = claim_start.full_date

left join {{ ref('reference_data__calendar') }} claim_end
on mc.claim_end_date = claim_end.full_date

left join {{ ref('reference_data__calendar') }} claim_line_start
on mc.claim_line_start_date = claim_line_start.full_date

left join {{ ref('reference_data__calendar') }} claim_line_end
on mc.claim_line_end_date = claim_line_end.full_date

left join {{ ref('terminology__icd_10_cm') }} icd1
on mc.diagnosis_code_1 = icd1.icd_10_cm

left join {{ ref('terminology__icd_10_cm') }} icd2
on mc.diagnosis_code_2 = icd2.icd_10_cm

left join {{ ref('terminology__icd_10_cm') }} icd3
on mc.diagnosis_code_3 = icd3.icd_10_cm

left join {{ ref('terminology__icd_10_cm') }} icd4
on mc.diagnosis_code_4 = icd4.icd_10_cm

left join {{ ref('terminology__icd_10_cm') }} icd5
on mc.diagnosis_code_5 = icd5.icd_10_cm

left join {{ ref('terminology__icd_10_cm') }} icd6
on mc.diagnosis_code_6 = icd6.icd_10_cm

left join {{ ref('terminology__icd_10_cm') }} icd7
on mc.diagnosis_code_7 = icd7.icd_10_cm

left join {{ ref('terminology__icd_10_cm') }} icd8
on mc.diagnosis_code_8 = icd8.icd_10_cm

left join {{ ref('terminology__icd_10_cm') }} icd9
on mc.diagnosis_code_9 = icd9.icd_10_cm

left join {{ ref('terminology__icd_10_cm') }} icd10
on mc.diagnosis_code_10 = icd10.icd_10_cm

left join {{ ref('terminology__icd_10_cm') }} icd11
on mc.diagnosis_code_11 = icd11.icd_10_cm

left join {{ ref('terminology__icd_10_cm') }} icd12
on mc.diagnosis_code_12 = icd12.icd_10_cm

left join {{ ref('terminology__icd_10_cm') }} icd13
on mc.diagnosis_code_13 = icd13.icd_10_cm

left join {{ ref('terminology__icd_10_cm') }} icd14
on mc.diagnosis_code_14 = icd14.icd_10_cm

left join {{ ref('terminology__icd_10_cm') }} icd15
on mc.diagnosis_code_15 = icd15.icd_10_cm

left join {{ ref('terminology__icd_10_cm') }} icd16
on mc.diagnosis_code_16 = icd16.icd_10_cm

left join {{ ref('terminology__icd_10_cm') }} icd17
on mc.diagnosis_code_17 = icd17.icd_10_cm

left join {{ ref('terminology__icd_10_cm') }} icd18
on mc.diagnosis_code_18 = icd18.icd_10_cm

left join {{ ref('terminology__icd_10_cm') }} icd19
on mc.diagnosis_code_19 = icd19.icd_10_cm

left join {{ ref('terminology__icd_10_cm') }} icd20
on mc.diagnosis_code_20 = icd20.icd_10_cm

left join {{ ref('terminology__icd_10_cm') }} icd21
on mc.diagnosis_code_21 = icd21.icd_10_cm

left join {{ ref('terminology__icd_10_cm') }} icd22
on mc.diagnosis_code_22 = icd22.icd_10_cm

left join {{ ref('terminology__icd_10_cm') }} icd23
on mc.diagnosis_code_23 = icd23.icd_10_cm

left join {{ ref('terminology__icd_10_cm') }} icd24
on mc.diagnosis_code_24 = icd24.icd_10_cm

left join {{ ref('terminology__icd_10_cm') }} icd25
on mc.diagnosis_code_25 = icd25.icd_10_cm

