

with group_by_types_of_rb_claims as (
select
  basic,
  hospice,
  loa,
  behavioral,
  count(*) as claim_count,
  count(*) * 100.0 / (select count(*) from {{ ref('rb_claims') }} ) as claim_percent

from {{ ref('rb_claims') }}
group by basic, hospice, loa, behavioral
)


select *
from group_by_types_of_rb_claims
