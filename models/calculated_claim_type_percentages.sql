

select 
  calculated_claim_type,
  count(*) as total_claims,
  round(
  count(*) * 100.0 / (select count(*) from {{ ref('claim_type') }}), 1)
  as percent_of_claims
from {{ ref('claim_type') }}
group by calculated_claim_type
