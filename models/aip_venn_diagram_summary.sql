
with rb as (
select count(*)
from {{ ref('aip_venn_diagram') }}
where rb = 1 and drg = 0 and bill = 0
),


drg as (
select count(*)
from {{ ref('aip_venn_diagram') }}
where rb = 0 and drg = 1 and bill = 0
),


bill as (
select count(*)
from {{ ref('aip_venn_diagram') }}
where rb = 0 and drg = 0 and bill = 1
),


rb_drg as (
select count(*)
from {{ ref('aip_venn_diagram') }}
where rb = 1 and drg = 1 and bill = 0
),


rb_bill as (
select count(*)
from {{ ref('aip_venn_diagram') }}
where rb = 1 and drg = 0 and bill = 1
),


drg_bill as (
select count(*)
from {{ ref('aip_venn_diagram') }}
where rb = 0 and drg = 1 and bill = 1
),


rb_drg_bill as (
select count(*)
from {{ ref('aip_venn_diagram') }}
where rb = 1 and drg = 1 and bill = 1
),


summary_cte as (

select
  1 as index,
  'rb' as venn_section,
  (select * from rb) as metric

union all

select
  2 as index,
  'drg' as venn_section,
  (select * from drg) as metric

union all

select
  3 as index,
  'bill' as venn_section,
  (select * from bill) as metric

union all

select
  4 as index,
  'rb_drg' as venn_section,
  (select * from rb_drg) as metric

union all

select
  5 as index,
  'rb_bill' as venn_section,
  (select * from rb_bill) as metric

union all

select
  6 as index,
  'drg_bill' as venn_section,
  (select * from drg_bill) as metric

union all

select
  7 as index,
  'rb_drg_bill' as venn_section,
  (select * from rb_drg_bill) as metric

)


select *
from summary_cte
