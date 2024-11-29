


with list_of_claims as (
select distinct claim_id
from {{ ref('valid_discharge_disposition_code_counts') }}
where valid_discharge_disposition_code_count = 1
)


select
  claim_id,
  discharge_disposition_code
from {{ ref('how_often_each_discharge_disposition_code_occurs') }}
where claim_id in (select * from list_of_claims)
and ranking = 1
