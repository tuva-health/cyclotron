

with unique_codes as (
select
  claim_id,
  discharge_disposition_code,
  'unique' as assignment_method
from {{ ref('unique_discharge_disposition_code') }}
),


determinable_codes as (
select
  claim_id,
  discharge_disposition_code_1 as discharge_disposition_code,
  'determinable' as assignment_method
from {{ ref('determinable_discharge_disposition_code') }}
),


undeterminable_codes as (
select
  claim_id,
  null as discharge_disposition_code,
  'undeterminable' as assignment_method
from {{ ref('undeterminable_discharge_disposition_code') }}
),


union_of_codes as (
select claim_id, discharge_disposition_code, assignment_method
from unique_codes

union all

select claim_id, discharge_disposition_code, assignment_method
from determinable_codes

union all

select claim_id, discharge_disposition_code, assignment_method
from undeterminable_codes
)



select *
from union_of_codes

