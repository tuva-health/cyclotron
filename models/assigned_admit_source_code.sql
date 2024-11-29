

with unique_codes as (
select
  claim_id,
  admit_source_code,
  'unique' as assignment_method
from {{ ref('unique_admit_source_code') }}
),


determinable_codes as (
select
  claim_id,
  admit_source_code_1 as admit_source_code,
  'determinable' as assignment_method
from {{ ref('determinable_admit_source_code') }}
),


undeterminable_codes as (
select
  claim_id,
  null as admit_source_code,
  'undeterminable' as assignment_method
from {{ ref('undeterminable_admit_source_code') }}
),


union_of_codes as (
select claim_id, admit_source_code, assignment_method
from unique_codes

union all

select claim_id, admit_source_code, assignment_method
from determinable_codes

union all

select claim_id, admit_source_code, assignment_method
from undeterminable_codes
)



select *
from union_of_codes

