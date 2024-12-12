
with medical_claim_rows as (
select count(*) 
from {{ ref('medical_claim') }}
),

medical_claim_distinct_pk as (
select count(distinct claim_id, claim_line_number) 
from {{ ref('medical_claim') }}
),

pharmacy_claim_rows as (
select count(*) 
from {{ ref('pharmacy_claim') }}
),

pharmacy_claim_distinct_pk as (
select count(distinct claim_id, claim_line_number) 
from {{ ref('pharmacy_claim') }}
),

eligibility_rows as (
select count(*) 
from {{ ref('eligibility') }}
),

eligibility_distinct_pk as (
select count(distinct patient_id, plan, enrollment_start_date, enrollment_end_date)
from {{ ref('eligibility') }}
)




select
'medical_claim has correct PK' as field,
case
  when
     (select * from medical_claim_rows) =
     (select * from medical_claim_distinct_pk) then 'YES'
  else 'NO'
end as field_value


union all


select
'pharmacy_claim has correct PK' as field,
case
  when
     (select * from pharmacy_claim_rows) =
     (select * from pharmacy_claim_distinct_pk) then 'YES'
  else 'NO'
end as field_value


union all


select
'eligibility has correct PK' as field,
case
  when
     (select * from eligibility_rows) =
     (select * from eligibility_distinct_pk) then 'YES'
  else 'NO'
end as field_value

