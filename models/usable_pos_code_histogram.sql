

with usable_pos_counts as (
select
  claim_id,
  count(distinct place_of_service_code) as usable_pos_count
from {{ ref('pos_all') }}
where valid_place_of_service_code = 1 and calculated_claim_type = 'professional'
group by claim_id
),

total_professional_claims as (
select total_claims
from {{ ref('calculated_claim_type_percentages') }}
where calculated_claim_type = 'professional'
),

one_or_more_pos_codes as (
select count(distinct claim_id) 
from usable_pos_counts
where usable_pos_count >= 1
),

two_or_more_pos_codes as (
select count(distinct claim_id) 
from usable_pos_counts
where usable_pos_count >= 2
),

three_or_more_pos_codes as (
select count(distinct claim_id) 
from usable_pos_counts
where usable_pos_count >= 3
),

four_or_more_pos_codes as (
select count(distinct claim_id) 
from usable_pos_counts
where usable_pos_count >= 4
),

five_or_more_pos_codes as (
select count(distinct claim_id) 
from usable_pos_counts
where usable_pos_count >= 5
),

six_or_more_pos_codes as (
select count(distinct claim_id) 
from usable_pos_counts
where usable_pos_count >= 6
),

seven_or_more_pos_codes as (
select count(distinct claim_id) 
from usable_pos_counts
where usable_pos_count >= 7
),

eight_or_more_pos_codes as (
select count(distinct claim_id) 
from usable_pos_counts
where usable_pos_count >= 8
),

nine_or_more_pos_codes as (
select count(distinct claim_id) 
from usable_pos_counts
where usable_pos_count >= 9
),

ten_or_more_pos_codes as (
select count(distinct claim_id) 
from usable_pos_counts
where usable_pos_count >= 10
)


select
'Claims with >=1 usable pos code' as field,
(select * from one_or_more_pos_codes) as number_of_claims,
round( (select * from one_or_more_pos_codes) * 100.0 /
(select * from total_professional_claims), 1)  as percent_of_professional_claims

union all 

select
'Claims with >=2 usable pos code' as field,
(select * from two_or_more_pos_codes) as number_of_claims,
round( (select * from two_or_more_pos_codes) * 100.0 /
(select * from total_professional_claims) , 1) as percent_of_professional_claims


union all

select
'Claims with >=3 usable pos code' as field,
(select * from three_or_more_pos_codes) as number_of_claims,
round( (select * from three_or_more_pos_codes) * 100.0 /
(select * from total_professional_claims) , 1) as percent_of_professional_claims


union all

select
'Claims with >=4 usable pos code' as field,
(select * from four_or_more_pos_codes) as number_of_claims,
round( (select * from four_or_more_pos_codes) * 100.0 /
(select * from total_professional_claims) , 1) as percent_of_professional_claims


union all

select
'Claims with >=5 usable pos code' as field,
(select * from five_or_more_pos_codes) as number_of_claims,
round( (select * from five_or_more_pos_codes) * 100.0 /
(select * from total_professional_claims) , 1) as percent_of_professional_claims


union all

select
'Claims with >=6 usable pos code' as field,
(select * from six_or_more_pos_codes) as number_of_claims,
round( (select * from six_or_more_pos_codes) * 100.0 /
(select * from total_professional_claims) , 1) as percent_of_professional_claims


union all

select
'Claims with >=7 usable pos code' as field,
(select * from seven_or_more_pos_codes) as number_of_claims,
round( (select * from seven_or_more_pos_codes) * 100.0 /
(select * from total_professional_claims) , 1) as percent_of_professional_claims


union all

select
'Claims with >=8 usable pos code' as field,
(select * from eight_or_more_pos_codes) as number_of_claims,
round( (select * from eight_or_more_pos_codes) * 100.0 /
(select * from total_professional_claims) , 1) as percent_of_professional_claims


union all

select
'Claims with >=9 usable pos code' as field,
(select * from nine_or_more_pos_codes) as number_of_claims,
round( (select * from nine_or_more_pos_codes) * 100.0 /
(select * from total_professional_claims) , 1) as percent_of_professional_claims


union all

select
'Claims with >=10 usable pos code' as field,
(select * from ten_or_more_pos_codes) as number_of_claims,
round( (select * from ten_or_more_pos_codes) * 100.0 /
(select * from total_professional_claims) , 1) as percent_of_professional_claims
