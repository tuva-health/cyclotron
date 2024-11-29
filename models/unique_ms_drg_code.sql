


with list_of_claims as (
select distinct claim_id
from {{ ref('valid_ms_drg_code_counts') }}
where valid_ms_drg_code_count = 1
)


select
  claim_id,
  ms_drg_code
from {{ ref('how_often_each_ms_drg_code_occurs') }}
where claim_id in (select * from list_of_claims)
and ranking = 1
