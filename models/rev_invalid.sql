

select
  revenue_center_code as invalid_rev,
  count(distinct claim_id) as number_of_claims
from {{ ref('rev_all') }}
where valid_revenue_center_code = 0
group by revenue_center_code
