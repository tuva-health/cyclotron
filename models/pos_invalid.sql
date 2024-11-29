

select
  place_of_service_code as invalid_pos,
  count(distinct claim_id) as number_of_claims
from {{ ref('pos_all') }}
where valid_place_of_service_code = 0
group by place_of_service_code
