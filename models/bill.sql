


select distinct claim_id
from {{ ref('header_values') }}
where usable_bill_type_code = 1 and substr(assigned_bill_type_code,1,2) in ('11','12')
