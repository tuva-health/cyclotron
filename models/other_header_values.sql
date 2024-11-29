
select
  claim_id,
  
  case
    when (
           ( max(patient_id) is not null) and 
           ( max(patient_id) = min(patient_id) )
	 ) then max(patient_id)
    else null
  end as patient_id,

  case
    when (
           ( max(patient_id) is not null) and 
           ( max(patient_id) = min(patient_id) )
	 ) then 1
    else 0
  end as usable_patient_id,

  case
    when (
           ( max(facility_npi) is not null) and 
           ( max(facility_npi) = min(facility_npi) )
	 ) then max(facility_npi)
    else null
  end as facility_npi,

  case
    when (
           ( max(facility_npi) is not null) and 
           ( max(facility_npi) = min(facility_npi) )
	 ) then 1
    else 0
  end as usable_facility_npi,

  case
    when (
           ( max(rendering_npi) is not null) and 
           ( max(rendering_npi) = min(rendering_npi) )
	 ) then max(rendering_npi)
    else null
  end as rendering_npi,

  case
    when (
           ( max(rendering_npi) is not null) and 
           ( max(rendering_npi) = min(rendering_npi) )
	 ) then 1
    else 0
  end as usable_rendering_npi,

  sum(paid_amount) as paid_amount


from {{ ref('medical_claim') }}
group by claim_id
