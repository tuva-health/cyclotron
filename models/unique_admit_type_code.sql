


with list_of_claims as (
select distinct claim_id
from {{ ref('valid_admit_type_code_counts') }}
where valid_admit_type_code_count = 1
)


select
  claim_id,
  admit_type_code
from {{ ref('how_often_each_admit_type_code_occurs') }}
where claim_id in (select * from list_of_claims)
and ranking = 1
