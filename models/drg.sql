


select distinct claim_id
from {{ ref('header_values') }}
where usable_ms_drg_code = 1 or usable_apr_drg_code = 1
