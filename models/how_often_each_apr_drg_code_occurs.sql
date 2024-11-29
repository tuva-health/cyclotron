
with add_count_of_occurrences as (
select
  claim_id,
  apr_drg_code,
  count(*) as occurrences
  
from {{ ref('valid_values') }}
where valid_apr_drg_code = 1

group by claim_id, apr_drg_code
),


add_ranking_from_high_to_low_occurrences as (
select
  claim_id,
  apr_drg_code,
  occurrences,
  row_number() over (
    partition by claim_id order by occurrences desc
  ) as ranking 
from add_count_of_occurrences
)


select *
from add_ranking_from_high_to_low_occurrences
