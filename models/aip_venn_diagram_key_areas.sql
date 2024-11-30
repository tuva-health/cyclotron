
with rb as (
select count(*)
from {{ ref('aip_venn_diagram') }}
where rb = 1 
),


drg as (
select count(*)
from {{ ref('aip_venn_diagram') }}
where drg = 1 
),


bill as (
select count(*)
from {{ ref('aip_venn_diagram') }}
where bill = 1
),

rb_drg_bill as (
select count(*)
from {{ ref('aip_venn_diagram') }}
where rb = 1 and drg = 1 and bill = 1
)


select
'Claims with bill_type_code in {11X, 12X}' as field,
(select * from bill) as number_of_claims,
round( (select * from bill) * 100.0 /
     ( select total_claims
       from {{ ref('calculated_claim_type_percentages') }}
       where calculated_claim_type = 'institutional'
     ),1) as percent_of_institutional_claims


union all


select
'Claims with room & board rev code' as field,
(select * from rb) as number_of_claims,
round( (select * from rb) * 100.0 /
     ( select total_claims
       from {{ ref('calculated_claim_type_percentages') }}
       where calculated_claim_type = 'institutional'
     ),1) as percent_of_institutional_claims


union all


select
'Claims with valid DRG' as field,
(select * from drg) as number_of_claims,
round( (select * from drg) * 100.0 /
     ( select total_claims
       from {{ ref('calculated_claim_type_percentages') }}
       where calculated_claim_type = 'institutional'
     ),1) as percent_of_institutional_claims


union all


select
'Claims with all 3' as field,
(select * from rb_drg_bill) as number_of_claims,
round( (select * from rb_drg_bill) * 100.0 /
     ( select total_claims
       from {{ ref('calculated_claim_type_percentages') }}
       where calculated_claim_type = 'institutional'
     ),1) as percent_of_institutional_claims




