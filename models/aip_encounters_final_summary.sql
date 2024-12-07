
with aip_encounters as (
select count(*)
from {{ ref('aip_encounters_final') }}
),


aip_encounters_with_dq_problems as (
select count(*)
from {{ ref('aip_encounters_final') }}
where dq_problem = 1
),

aip_encounters_with_unusable_dx1 as (
select count(*)
from {{ ref('aip_encounters_final') }}
where usable_diagnosis_code_1 = 0
),

aip_encounters_with_unusable_atc as (
select count(*)
from {{ ref('aip_encounters_final') }}
where usable_admit_type_code = 0
),

aip_encounters_with_unusable_asc as (
select count(*)
from {{ ref('aip_encounters_final') }}
where usable_admit_source_code = 0
),

aip_encounters_with_unusable_ddc as (
select count(*)
from {{ ref('aip_encounters_final') }}
where usable_discharge_disposition_code = 0
),

aip_encounters_with_unusable_facility_npi as (
select count(*)
from {{ ref('aip_encounters_final') }}
where usable_facility_npi = 0
),

aip_encounters_with_unusable_rendering_npi as (
select count(*)
from {{ ref('aip_encounters_final') }}
where usable_rendering_npi = 0
),

single_inst_claim_aip_encounters as (
select count(*)
from {{ ref('aip_encounters_final') }}
where single_claim_encounter = 1
),

multiple_inst_claim_aip_encounters as (
select count(*)
from {{ ref('aip_encounters_final') }}
where multi_claim_encounter = 1
),

aip_encounters_with_prof_claims as (
select count(*)
from {{ ref('aip_encounters_final') }}
where has_professional_claims = 1
),

aip_encounters_without_prof_claims as (
select count(*)
from {{ ref('aip_encounters_final') }}
where has_professional_claims = 0
),

spend_from_prof_claims as (
select sum(professional_paid_amount)
from {{ ref('aip_encounters_final') }}
where has_professional_claims = 1
),

total_spend_on_aip_encounters_with_prof_claims as (
select round( sum(total_paid_amount) , 1)
from {{ ref('aip_encounters_final') }}
where has_professional_claims = 1
),

aip_encounters_with_death as (
select count(*)
from {{ ref('aip_encounters_final') }}
where discharge_disposition_code in ('20','40','41')
),

average_los as (
select round(avg(los),1)
from {{ ref('aip_encounters_final') }}
),

average_total_paid_amount as (
select round(avg(total_paid_amount),1)
from {{ ref('aip_encounters_final') }}
),

average_total_paid_amount as (
select round(avg(total_paid_amount),1)
from {{ ref('aip_encounters_final') }}
)


select
'aip_encounters' as field,
(select * from aip_encounters) as field_value

union all

select
'(aip_encounters_with_dq_prob) / (aip_encounters) * 100' as field,
round( (select * from aip_encounters_with_dq_problems) * 100.0 /
(select * from aip_encounters),1) as field_value

union all

select
'(aip_encounters_with_unusable_dx1) / (aip_encounters) * 100' as field,
round( (select * from aip_encounters_with_unusable_dx1) * 100.0 /
(select * from aip_encounters),1) as field_value

union all

select
'(aip_encounters_with_unusable_atc) / (aip_encounters) * 100' as field,
round( (select * from aip_encounters_with_unusable_atc) * 100.0 /
(select * from aip_encounters),1) as field_value

union all

select
'(aip_encounters_with_unusable_asc) / (aip_encounters) * 100' as field,
round( (select * from aip_encounters_with_unusable_asc) * 100.0 /
(select * from aip_encounters),1) as field_value

union all

select
'(aip_encounters_with_unusable_ddc) / (aip_encounters) * 100' as field,
round( (select * from aip_encounters_with_unusable_ddc) * 100.0 /
(select * from aip_encounters),1) as field_value

union all

select
'(aip_encounters_with_unusable_facility_npi) / (aip_encounters) * 100' as field,
round( (select * from aip_encounters_with_unusable_facility_npi) * 100.0 /
(select * from aip_encounters),1) as field_value

union all

select
'(aip_encounters_with_unusable_rendering_npi) / (aip_encounters) * 100' as field,
round( (select * from aip_encounters_with_unusable_rendering_npi) * 100.0 /
(select * from aip_encounters),1) as field_value

union all

select
'(single_inst_claim_aip_encounters) / (aip_encounters) * 100' as field,
round( (select * from single_inst_claim_aip_encounters) * 100.0 /
(select * from aip_encounters),1) as field_value

union all

select
'(multiple_inst_claim_aip_encounters) / (aip_encounters) * 100' as field,
round( (select * from multiple_inst_claim_aip_encounters) * 100.0 /
(select * from aip_encounters),1) as field_value

union all

select
'(aip_encounters_with_prof_claims) / (aip_encounters) * 100' as field,
round( (select * from aip_encounters_with_prof_claims) * 100.0 /
(select * from aip_encounters),1) as field_value

union all

select
'(aip_encounters_without_prof_claims) / (aip_encounters) * 100' as field,
round( (select * from aip_encounters_without_prof_claims) * 100.0 /
(select * from aip_encounters),1) as field_value

union all

select
'(spend_from_prof_claims) / (total_spend_on_aip_encounters_with_prof_claims) * 100' as field,
round( (select * from spend_from_prof_claims) * 1.0 /
(select * from total_spend_on_aip_encounters_with_prof_claims),1) as field_value

union all

select
'(aip_encounters_with_death) / (aip_encounters) * 100' as field,
round( (select * from aip_encounters_with_death) * 100.0 /
(select * from aip_encounters),1) as field_value

union all

select
'average_los' as field,
(select * from average_los) as field_value

union all

select
'average_total_paid_amount' as field,
(select * from average_total_paid_amount) as field_value

