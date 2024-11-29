

select
  claim_id,
  count(distinct diagnosis_code_1) as valid_diagnosis_code_1_count
  
from {{ ref('valid_values') }}
where valid_diagnosis_code_1 = 1

group by claim_id
