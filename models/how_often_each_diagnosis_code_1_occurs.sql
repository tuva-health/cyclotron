
with add_count_of_occurrences as (
select
  claim_id,
  diagnosis_code_1,
  count(*) as occurrences
  
from {{ ref('valid_values') }}
where valid_diagnosis_code_1 = 1

group by claim_id, diagnosis_code_1
),


add_ranking_from_high_to_low_occurrences as (
select
  claim_id,
  diagnosis_code_1,
  occurrences,
  row_number() over (
    partition by claim_id order by occurrences desc
  ) as ranking 
from add_count_of_occurrences
)


select *
from add_ranking_from_high_to_low_occurrences
