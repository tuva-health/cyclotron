

select 
  calculated_claim_type,

  count(*) as total_claims,
  
  round(
    count(*) * 100.0 / (select count(*) from {{ ref('claim_type') }}), 1)
  as percent_of_claims,


  has_institutional_fields,
  has_valid_institutional_fields,
  has_professional_fields,
  has_valid_professional_fields

from {{ ref('claim_type') }}
group by 
  calculated_claim_type,
  has_institutional_fields,
  has_valid_institutional_fields,
  has_professional_fields,
  has_valid_professional_fields

