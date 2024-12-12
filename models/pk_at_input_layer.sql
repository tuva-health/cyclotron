

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
'select count(*) from medical_claim' as field,
(select * from medical_claim_rows) as field_value

union all

select
'select count(distinct claim_id, claim_line_number) from medical_claim' as field,
(select * from medical_claim_distinct_pk) as field_value

union all

select
'select count(*) from pharmacy_claim' as field,
(select * from pharmacy_claim_rows) as field_value

union all

select
'select count(distinct claim_id, claim_line_number) from pharmacy_claim' as field,
(select * from pharmacy_claim_distinct_pk) as field_value

union all

select
'select count(*) from eligibility' as field,
(select * from eligibility_rows) as field_value

union all

select
'select count(distinct patient_id, plan, enrollment_start_date, enrollment_end_date) from eligibility' as field,
(select * from eligibility_distinct_pk) as field_value
