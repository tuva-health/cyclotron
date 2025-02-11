

with list_of_claims as (
select distinct claim_id
from {{ ref('valid_admit_source_code_counts') }}
where valid_admit_source_code_count > 1
),

claims_with_multiple_valid_values_ranking_1 as (
select
  claim_id,
  admit_source_code,
  occurrences,
  ranking
from {{ ref('how_often_each_admit_source_code_occurs') }}
where ranking = 1 and claim_id in (select * from list_of_claims)
),

claims_with_multiple_valid_values_ranking_2 as (
select
  claim_id,
  admit_source_code,
  occurrences,
  ranking
from {{ ref('how_often_each_admit_source_code_occurs') }}
where ranking = 2 and claim_id in (select * from list_of_claims)
),


determinable as (
select
  aa.claim_id,
  aa.admit_source_code as admit_source_code_1,
  aa.occurrences as occurrences_1,
  bb.admit_source_code as admit_source_code_2,
  bb.occurrences as occurrences_2
  
from claims_with_multiple_valid_values_ranking_1 aa
left join claims_with_multiple_valid_values_ranking_2 bb
on aa.claim_id = bb.claim_id

where aa.occurrences = bb.occurrences
)


select *
from determinable
