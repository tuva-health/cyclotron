

with unique_codes as (
select
  claim_id,
  apr_drg_code,
  'unique' as assignment_method
from {{ ref('unique_apr_drg_code') }}
),


determinable_codes as (
select
  claim_id,
  apr_drg_code_1 as apr_drg_code,
  'determinable' as assignment_method
from {{ ref('determinable_apr_drg_code') }}
),


undeterminable_codes as (
select
  claim_id,
  null as apr_drg_code,
  'undeterminable' as assignment_method
from {{ ref('undeterminable_apr_drg_code') }}
),


union_of_codes as (
select claim_id, apr_drg_code, assignment_method
from unique_codes

union all

select claim_id, apr_drg_code, assignment_method
from determinable_codes

union all

select claim_id, apr_drg_code, assignment_method
from undeterminable_codes
)



select *
from union_of_codes

