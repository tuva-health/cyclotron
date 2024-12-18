




select
1 as rank_id,
'Bill Type Code atomic data quality:' as field,
null as field_value


union all


select
2 as rank_id,
field,
field_value
from {{ ref('header_values_graph') }}
where field = '(inst claims with missing bill type) / (total inst claims) * 100'


union all


select
3 as rank_id,
field,
field_value
from {{ ref('header_values_graph') }}
where field = '(inst claims with populated bill type) / (total inst claims) * 100'


union all


select
4 as rank_id,
field,
field_value
from {{ ref('header_values_graph') }}
where field = '(inst claims with always valid bill type) / (total inst claims) * 100'


union all


select
5 as rank_id,
field,
field_value
from {{ ref('header_values_graph') }}
where field = '(inst claims with valid and invalid bill type) / (total inst claims) * 100'


union all


select
6 as rank_id,
field,
field_value
from {{ ref('header_values_graph') }}
where field = '(inst claims with always invalid bill type) / (total inst claims) * 100'


union all


select
7 as rank_id,
field,
field_value
from {{ ref('header_values_graph') }}
where field = '(inst claims with undeterminable bill type) / (total inst claims) * 100'


union all


select
8 as rank_id,
field,
field_value
from {{ ref('header_values_graph') }}
where field = '(inst claims with determinable bill type) / (total inst claims) * 100'


union all


select
9 as rank_id,
field,
field_value
from {{ ref('header_values_graph') }}
where field = '(inst claims with unique bill type) / (total inst claims) * 100'


union all


select
10 as rank_id,
field,
field_value
from {{ ref('header_values_graph') }}
where field = '(inst claims with usable bill type) / (total inst claims) * 100'


union all


select
11 as rank_id,
null as field,
null as field_value


union all


select
12 as rank_id,
'MS-DRG atomic data quality:' as field,
null as field_value
 

union all


select
13 as rank_id,
field,
field_value
from {{ ref('header_values_graph') }}
where field = '(inst claims with missing ms-drg) / (total inst claims) * 100'
 

union all


select
14 as rank_id,
field,
field_value
from {{ ref('header_values_graph') }}
where field = '(inst claims with populated ms-drg) / (total inst claims) * 100'
 

union all


select
15 as rank_id,
field,
field_value
from {{ ref('header_values_graph') }}
where field = '(inst claims with always valid ms-drg) / (total inst claims) * 100'
 

union all


select
16 as rank_id,
field,
field_value
from {{ ref('header_values_graph') }}
where field = '(inst claims with valid and invalid ms-drg) / (total inst claims) * 100'
 

union all


select
17 as rank_id,
field,
field_value
from {{ ref('header_values_graph') }}
where field = '(inst claims with always invalid ms-drg) / (total inst claims) * 100'
 

union all


select
18 as rank_id,
field,
field_value
from {{ ref('header_values_graph') }}
where field = '(inst claims with undeterminable ms-drg) / (total inst claims) * 100'
 

union all


select
19 as rank_id,
field,
field_value
from {{ ref('header_values_graph') }}
where field = '(inst claims with determinable ms-drg) / (total inst claims) * 100'
 

union all


select
20 as rank_id,
field,
field_value
from {{ ref('header_values_graph') }}
where field = '(inst claims with unique ms-drg) / (total inst claims) * 100'
 

union all


select
21 as rank_id,
field,
field_value
from {{ ref('header_values_graph') }}
where field = '(inst claims with usable ms-drg) / (total inst claims) * 100'


union all


select
22 as rank_id,
null as field,
null as field_value


union all


select
23 as rank_id,
'APR-DRG atomic data quality:' as field,
null as field_value
 

union all


select
24 as rank_id,
field,
field_value
from {{ ref('header_values_graph') }}
where field = '(inst claims with missing apr-drg) / (total inst claims) * 100'
 

union all


select
25 as rank_id,
field,
field_value
from {{ ref('header_values_graph') }}
where field = '(inst claims with populated apr-drg) / (total inst claims) * 100'
 

union all


select
26 as rank_id,
field,
field_value
from {{ ref('header_values_graph') }}
where field = '(inst claims with always valid apr-drg) / (total inst claims) * 100'
 

union all


select
27 as rank_id,
field,
field_value
from {{ ref('header_values_graph') }}
where field = '(inst claims with valid and invalid apr-drg) / (total inst claims) * 100'
 

union all


select
28 as rank_id,
field,
field_value
from {{ ref('header_values_graph') }}
where field = '(inst claims with always invalid apr-drg) / (total inst claims) * 100'
 

union all


select
29 as rank_id,
field,
field_value
from {{ ref('header_values_graph') }}
where field = '(inst claims with undeterminable apr-drg) / (total inst claims) * 100'
 

union all


select
30 as rank_id,
field,
field_value
from {{ ref('header_values_graph') }}
where field = '(inst claims with determinable apr-drg) / (total inst claims) * 100'
 

union all


select
31 as rank_id,
field,
field_value
from {{ ref('header_values_graph') }}
where field = '(inst claims with unique apr-drg) / (total inst claims) * 100'
 

union all


select
32 as rank_id,
field,
field_value
from {{ ref('header_values_graph') }}
where field = '(inst claims with usable apr-drg) / (total inst claims) * 100'


union all


select
33 as rank_id,
null as field,
null as field_value


union all


select
34 as rank_id,
'Revenue Center atomic data quality:' as field,
null as field_value


union all


select
35 as rank_id,
'(valid rev codes) / (all rev codes) * 100' as field,
round(
(select count(*) from {{ ref('rev_all') }} where valid_revenue_center_code = 1) * 100.0 /
(select count(*) from {{ ref('rev_all') }} )
, 1) as field_value


union all


select
36 as rank_id,
'(claims with >= 1 usable rev code) / (inst claims) * 100' as field,
(select percent_of_institutional_claims
 from {{ ref('usable_rev_code_histogram') }}
 where field = 'Claims with >=1 usable rev code') as field_value


union all


select
37 as rank_id,
'(claims with >= 2 usable rev code) / (inst claims) * 100' as field,
(select percent_of_institutional_claims
 from {{ ref('usable_rev_code_histogram') }}
 where field = 'Claims with >=2 usable rev code') as field_value


union all


select
38 as rank_id,
'(claims with >= 3 usable rev code) / (inst claims) * 100' as field,
(select percent_of_institutional_claims
 from {{ ref('usable_rev_code_histogram') }}
 where field = 'Claims with >=3 usable rev code') as field_value


union all


select
39 as rank_id,
'(claims with >= 4 usable rev code) / (inst claims) * 100' as field,
(select percent_of_institutional_claims
 from {{ ref('usable_rev_code_histogram') }}
 where field = 'Claims with >=4 usable rev code') as field_value


union all


select
40 as rank_id,
'(claims with >= 5 usable rev code) / (inst claims) * 100' as field,
(select percent_of_institutional_claims
 from {{ ref('usable_rev_code_histogram') }}
 where field = 'Claims with >=5 usable rev code') as field_value


union all


select
41 as rank_id,
'(claims with >= 6 usable rev code) / (inst claims) * 100' as field,
(select percent_of_institutional_claims
 from {{ ref('usable_rev_code_histogram') }}
 where field = 'Claims with >=6 usable rev code') as field_value



union all


select
42 as rank_id,
'(claims with >= 7 usable rev code) / (inst claims) * 100' as field,
(select percent_of_institutional_claims
 from {{ ref('usable_rev_code_histogram') }}
 where field = 'Claims with >=7 usable rev code') as field_value



union all


select
43 as rank_id,
'(claims with >= 8 usable rev code) / (inst claims) * 100' as field,
(select percent_of_institutional_claims
 from {{ ref('usable_rev_code_histogram') }}
 where field = 'Claims with >=8 usable rev code') as field_value



union all


select
44 as rank_id,
'(claims with >= 9 usable rev code) / (inst claims) * 100' as field,
(select percent_of_institutional_claims
 from {{ ref('usable_rev_code_histogram') }}
 where field = 'Claims with >=9 usable rev code') as field_value


union all


select
45 as rank_id,
'(claims with >= 10 usable rev code) / (inst claims) * 100' as field,
(select percent_of_institutional_claims
 from {{ ref('usable_rev_code_histogram') }}
 where field = 'Claims with >=10 usable rev code') as field_value


union all


select
46 as rank_id,
null as field,
null as field_value


union all


select
47 as rank_id,
'Venn Diagram for aip claims:' as field,
null as field_value


union all


select
48 as rank_id,
'rb claims' as field,
(select claims
 from {{ ref('aip_venn_diagram_summary') }}
 where venn_section = 'rb') as field_value


union all


select
49 as rank_id,
'drg claims' as field,
(select claims
 from {{ ref('aip_venn_diagram_summary') }}
 where venn_section = 'drg') as field_value


union all


select
50 as rank_id,
'bill claims' as field,
(select claims
 from {{ ref('aip_venn_diagram_summary') }}
 where venn_section = 'bill') as field_value


union all


select
51 as rank_id,
'rb_drg claims' as field,
(select claims
 from {{ ref('aip_venn_diagram_summary') }}
 where venn_section = 'rb_drg') as field_value


union all


select
52 as rank_id,
'rb_bill claims' as field,
(select claims
 from {{ ref('aip_venn_diagram_summary') }}
 where venn_section = 'rb_bill') as field_value


union all


select
53 as rank_id,
'drg_bill claims' as field,
(select claims
 from {{ ref('aip_venn_diagram_summary') }}
 where venn_section = 'drg_bill') as field_value


union all


select
54 as rank_id,
'rb_drg_bill claims' as field,
(select claims
 from {{ ref('aip_venn_diagram_summary') }}
 where venn_section = 'rb_drg_bill') as field_value


union all


select
55 as rank_id,
'(rb claims) / (inst claims) * 100' as field,
(select percent_of_institutional_claims
 from {{ ref('aip_venn_diagram_summary') }}
 where venn_section = 'rb') as field_value


union all


select
56 as rank_id,
'(drg claims) / (inst claims) * 100' as field,
(select percent_of_institutional_claims
 from {{ ref('aip_venn_diagram_summary') }}
 where venn_section = 'drg') as field_value


union all


select
57 as rank_id,
'(bill claims) / (inst claims) * 100' as field,
(select percent_of_institutional_claims
 from {{ ref('aip_venn_diagram_summary') }}
 where venn_section = 'bill') as field_value


union all


select
58 as rank_id,
'(rb_drg claims) / (inst claims) * 100' as field,
(select percent_of_institutional_claims
 from {{ ref('aip_venn_diagram_summary') }}
 where venn_section = 'rb_drg') as field_value


union all


select
59 as rank_id,
'(rb_bill claims) / (inst claims) * 100' as field,
(select percent_of_institutional_claims
 from {{ ref('aip_venn_diagram_summary') }}
 where venn_section = 'rb_bill') as field_value


union all


select
60 as rank_id,
'(drg_bill claims) / (inst claims) * 100' as field,
(select percent_of_institutional_claims
 from {{ ref('aip_venn_diagram_summary') }}
 where venn_section = 'drg_bill') as field_value


union all


select
61 as rank_id,
'(rb_drg_bill claims) / (inst claims) * 100' as field,
(select percent_of_institutional_claims
 from {{ ref('aip_venn_diagram_summary') }}
 where venn_section = 'rb_drg_bill') as field_value


union all


select
62 as rank_id,
'Claims with bill_type_code in {11X, 12X}' as field,
(select number_of_claims
 from {{ ref('aip_venn_diagram_key_areas') }}
 where field = 'Claims with bill_type_code in {11X, 12X}') as field_value


union all


select
63 as rank_id,
'Claims with room & board rev code' as field,
(select number_of_claims
 from {{ ref('aip_venn_diagram_key_areas') }}
 where field = 'Claims with room & board rev code') as field_value


union all


select
64 as rank_id,
'Claims with valid DRG' as field,
(select number_of_claims
 from {{ ref('aip_venn_diagram_key_areas') }}
 where field = 'Claims with valid DRG') as field_value


union all


select
65 as rank_id,
'Claims with bill_type_code in {11X, 12X} OR valid DRG' as field,
(select sum(claims)
 from {{ ref('aip_venn_diagram_summary') }}
 where venn_section in ('rb_drg',
                        'drg',
                        'rb_drg_bill',
			'rb_bill',
			'drg_bill',
			'bill')) as field_value


union all


select
66 as rank_id,
'(Claims with bill_type_code in {11X, 12X}) / (inst claims) * 100' as field,
(select percent_of_institutional_claims
 from {{ ref('aip_venn_diagram_key_areas') }}
 where field = 'Claims with bill_type_code in {11X, 12X}') as field_value


union all


select
67 as rank_id,
'(Claims with room & board rev code) / (inst claims) * 100' as field,
(select percent_of_institutional_claims
 from {{ ref('aip_venn_diagram_key_areas') }}
 where field = 'Claims with room & board rev code') as field_value


union all


select
68 as rank_id,
'(Claims with valid DRG) / (inst claims) * 100' as field,
(select percent_of_institutional_claims
 from {{ ref('aip_venn_diagram_key_areas') }}
 where field = 'Claims with valid DRG') as field_value


union all


select
69 as rank_id,
'(Claims with bill_type_code in {11X, 12X} OR valid DRG) / (inst claims) * 100' as field,
(select round(
    (select sum(claims)
     from {{ ref('aip_venn_diagram_summary') }}
     where venn_section in ('rb_drg',
                            'drg',
                            'rb_drg_bill',
        		    'rb_bill',
			    'drg_bill',
			    'bill')
    ) * 100.0 /
       (select total_claims
        from {{ ref('calculated_claim_type_percentages') }}
        where calculated_claim_type = 'institutional'
       )
, 1)
) as field_value


union all


select
70 as rank_id,
null as field,
null as field_value


union all


select
71 as rank_id,
'Acute inpatient institutional claims summary:' as field,
null as field_value


union all


select
72 as rank_id,
'total # of claims' as field,
(select field_value
 from {{ ref('aip_inst_claims_dq_summary') }}
 where field = 'total # of claims') as field_value


union all


select
73 as rank_id,
'# inst claims' as field,
(select field_value
 from {{ ref('aip_inst_claims_dq_summary') }}
 where field = '# inst claims') as field_value


union all


select
74 as rank_id,
'# AIP inst claims' as field,
(select field_value
 from {{ ref('aip_inst_claims_dq_summary') }}
 where field = '# AIP inst claims') as field_value


union all


select
75 as rank_id,
'(# AIP inst claims) / (# inst claims) * 100' as field,
(select field_value
 from {{ ref('aip_inst_claims_dq_summary') }}
 where field = '(# AIP inst claims) / (# inst claims) * 100') as field_value


union all


select
76 as rank_id,
'(# AIP inst claims) / (total # of claims) * 100' as field,
(select field_value
 from {{ ref('aip_inst_claims_dq_summary') }}
 where field = '(# AIP inst claims) / (total # of claims) * 100') as field_value


union all


select
77 as rank_id,
'(# usable AIP inst claims) / (# AIP inst claims) * 100' as field,
(select field_value
 from {{ ref('aip_inst_claims_dq_summary') }}
 where field = '(# usable AIP inst claims) / (# AIP inst claims) * 100') as field_value


union all


select
78 as rank_id,
'(# AIP inst claims with DQ problems) / (# AIP inst claims) * 100' as field,
(select field_value
 from {{ ref('aip_inst_claims_dq_summary') }}
 where field = '(# AIP inst claims with DQ problems) / (# AIP inst claims) * 100') as field_value


union all


select
79 as rank_id,
'(# AIP inst claims with unusable patient_id) / (# AIP inst claims) * 100' as field,
(select field_value
 from {{ ref('aip_inst_claims_dq_summary') }}
 where field = '(# AIP inst claims with unusable patient_id) / (# AIP inst claims) * 100') as field_value


union all


select
80 as rank_id,
'(# AIP inst claims with unusable merge dates) / (# AIP inst claims) * 100' as field,
(select field_value
 from {{ ref('aip_inst_claims_dq_summary') }}
 where field = '(# AIP inst claims with unusable merge dates) / (# AIP inst claims) * 100') as field_value


union all


select
81 as rank_id,
'(# AIP inst claims with unusable ms_drg_code) / (# AIP inst claims) * 100' as field,
(select field_value
 from {{ ref('aip_inst_claims_dq_summary') }}
 where field = '(# AIP inst claims with unusable ms_drg_code) / (# AIP inst claims) * 100') as field_value


union all


select
82 as rank_id,
'(# AIP inst claims with unusable apr_drg_code) / (# AIP inst claims) * 100' as field,
(select field_value
 from {{ ref('aip_inst_claims_dq_summary') }}
 where field = '(# AIP inst claims with unusable apr_drg_code) / (# AIP inst claims) * 100') as field_value


union all


select
83 as rank_id,
'(# AIP inst claims with unusable diagnosis_code_1) / (# AIP inst claims) * 100' as field,
(select field_value
 from {{ ref('aip_inst_claims_dq_summary') }}
 where field = '(# AIP inst claims with unusable diagnosis_code_1) / (# AIP inst claims) * 100') as field_value


union all


select
84 as rank_id,
'(# AIP inst claims with unusable ATC) / (# AIP inst claims) * 100' as field,
(select field_value
 from {{ ref('aip_inst_claims_dq_summary') }}
 where field = '(# AIP inst claims with unusable ATC) / (# AIP inst claims) * 100') as field_value


union all


select
85 as rank_id,
'(# AIP inst claims with unusable ASC) / (# AIP inst claims) * 100' as field,
(select field_value
 from {{ ref('aip_inst_claims_dq_summary') }}
 where field = '(# AIP inst claims with unusable ASC) / (# AIP inst claims) * 100') as field_value


union all


select
86 as rank_id,
'(# AIP inst claims with unusable DDC) / (# AIP inst claims) * 100' as field,
(select field_value
 from {{ ref('aip_inst_claims_dq_summary') }}
 where field = '(# AIP inst claims with unusable DDC) / (# AIP inst claims) * 100') as field_value


union all


select
87 as rank_id,
'(# AIP inst claims with unusable facility_npi) / (# AIP inst claims) * 100' as field,
(select field_value
 from {{ ref('aip_inst_claims_dq_summary') }}
 where field = '(# AIP inst claims with unusable facility_npi) / (# AIP inst claims) * 100') as field_value


union all


select
88 as rank_id,
'(# AIP inst claims with unusable rendering_npi) / (# AIP inst claims) * 100' as field,
(select field_value
 from {{ ref('aip_inst_claims_dq_summary') }}
 where field = '(# AIP inst claims with unusable rendering_npi) / (# AIP inst claims) * 100') as field_value


union all


select
89 as rank_id,
null as field,
null as field_value


union all


select
90 as rank_id,
'Constructiong AIP encounters' as field,
null as field_value


union all


select
91 as rank_id,
'Total AIP inst claims' as field,
(select count(*) from {{ ref('acute_inpatient_institutional_claims') }}) as field_value


union all


select
92 as rank_id,
'Usable AIP inst claims' as field,
(select count(*) from {{ ref('acute_inpatient_institutional_claims') }}
 where usable_for_aip_encounter = 1) as field_value


union all


select
93 as rank_id,
'AIP inst claims that make up single-claim encounters' as field,
(select count(*) from {{ ref('aip_single_claim_encounters') }}) as field_value



union all


select
94 as rank_id,
'AIP inst claims that make up multi-claim encounters' as field,
(select count(*) from {{ ref('aip_multiple_claim_encounters') }}) as field_value



union all


select
95 as rank_id,
'AIP encounters made up of multiple inst claims' as field,
(select count(*) from {{ ref('aip_multiple_claim_encounter_fields') }}) as field_value


union all


select
96 as rank_id,
null as field,
null as field_value


union all


select
97 as rank_id,
'Data Quality issues specific to multiple-claim encounters' as field,
null as field_value


union all


select
98 as rank_id,
'Encounters with a DQ problem' as field,
(select encounters
 from {{ ref('aip_multiple_claim_encounters_dq_summary') }}
 where field = 'Encounters with a DQ problem') as field_value


union all


select
99 as rank_id,
'Encounters with a multiple MS-DRG' as field,
(select encounters
 from {{ ref('aip_multiple_claim_encounters_dq_summary') }}
 where field = 'Encounters with a multiple MS-DRG') as field_value


union all


select
100 as rank_id,
'Encounters with a multiple APR-DRG' as field,
(select encounters
 from {{ ref('aip_multiple_claim_encounters_dq_summary') }}
 where field = 'Encounters with a multiple APR-DRG') as field_value


union all


select
101 as rank_id,
'Encounters with a multiple Dx1' as field,
(select encounters
 from {{ ref('aip_multiple_claim_encounters_dq_summary') }}
 where field = 'Encounters with a multiple Dx1') as field_value


union all


select
102 as rank_id,
'Encounters with a multiple ATC' as field,
(select encounters
 from {{ ref('aip_multiple_claim_encounters_dq_summary') }}
 where field = 'Encounters with a multiple ATC') as field_value


union all


select
103 as rank_id,
'Encounters with a multiple ASC' as field,
(select encounters
 from {{ ref('aip_multiple_claim_encounters_dq_summary') }}
 where field = 'Encounters with a multiple ASC') as field_value


union all


select
104 as rank_id,
'Encounters with a multiple DDC' as field,
(select encounters
 from {{ ref('aip_multiple_claim_encounters_dq_summary') }}
 where field = 'Encounters with a multiple DDC') as field_value


union all


select
105 as rank_id,
'Encounters with a multiple facility NPI' as field,
(select encounters
 from {{ ref('aip_multiple_claim_encounters_dq_summary') }}
 where field = 'Encounters with a multiple facility NPI') as field_value


union all


select
106 as rank_id,
'Encounters with a multiple rendering NPI' as field,
(select encounters
 from {{ ref('aip_multiple_claim_encounters_dq_summary') }}
 where field = 'Encounters with a multiple rendering NPI') as field_value


union all


select
107 as rank_id,
'(Encounters with a DQ problem) / (multi-claim enc) * 100' as field,
round(
(select encounters
 from {{ ref('aip_multiple_claim_encounters_dq_summary') }}
 where field = 'Encounters with a DQ problem') * 100 /
(select count(*) from {{ ref('aip_multiple_claim_encounter_fields') }}) , 1)
as field_value

union all


select
108 as rank_id,
'(Encounters with a multiple MS-DRG) / (multi-claim enc) * 100' as field,
round(
(select encounters
 from {{ ref('aip_multiple_claim_encounters_dq_summary') }}
 where field = 'Encounters with a multiple MS-DRG') * 100 /
(select count(*) from {{ ref('aip_multiple_claim_encounter_fields') }}) , 1)
as field_value


union all


select
109 as rank_id,
'(Encounters with a multiple APR-DRG) / (multi-claim enc) * 100' as field,
round(
(select encounters
 from {{ ref('aip_multiple_claim_encounters_dq_summary') }}
 where field = 'Encounters with a multiple APR-DRG') * 100 /
(select count(*) from {{ ref('aip_multiple_claim_encounter_fields') }}) , 1)
as field_value


union all


select
110 as rank_id,
'(Encounters with a multiple Dx1) / (multi-claim enc) * 100' as field,
round(
(select encounters
 from {{ ref('aip_multiple_claim_encounters_dq_summary') }}
 where field = 'Encounters with a multiple Dx1') * 100 /
(select count(*) from {{ ref('aip_multiple_claim_encounter_fields') }}) , 1)
as field_value


union all


select
111 as rank_id,
'(Encounters with a multiple ATC) / (multi-claim enc) * 100' as field,
round(
(select encounters
 from {{ ref('aip_multiple_claim_encounters_dq_summary') }}
 where field = 'Encounters with a multiple ATC') * 100 /
(select count(*) from {{ ref('aip_multiple_claim_encounter_fields') }}) , 1)
as field_value


union all


select
112 as rank_id,
'(Encounters with a multiple ASC) / (multi-claim enc) * 100' as field,
round(
(select encounters
 from {{ ref('aip_multiple_claim_encounters_dq_summary') }}
 where field = 'Encounters with a multiple ASC') * 100 /
(select count(*) from {{ ref('aip_multiple_claim_encounter_fields') }}) , 1)
as field_value


union all


select
113 as rank_id,
'(Encounters with a multiple DDC) / (multi-claim enc) * 100' as field,
round(
(select encounters
 from {{ ref('aip_multiple_claim_encounters_dq_summary') }}
 where field = 'Encounters with a multiple DDC') * 100 /
(select count(*) from {{ ref('aip_multiple_claim_encounter_fields') }}) , 1)
as field_value


union all


select
114 as rank_id,
'(Encounters with a multiple facility NPI) / (multi-claim enc) * 100' as field,
round(
(select encounters
 from {{ ref('aip_multiple_claim_encounters_dq_summary') }}
 where field = 'Encounters with a multiple facility NPI') * 100 /
(select count(*) from {{ ref('aip_multiple_claim_encounter_fields') }}) , 1)
as field_value


union all


select
115 as rank_id,
'(Encounters with a multiple rendering NPI) / (multi-claim enc) * 100' as field,
round(
(select encounters
 from {{ ref('aip_multiple_claim_encounters_dq_summary') }}
 where field = 'Encounters with a multiple rendering NPI') * 100 /
(select count(*) from {{ ref('aip_multiple_claim_encounter_fields') }}) , 1)
as field_value


union all


select
116 as rank_id,
null as field,
null as field_value


union all


select
117 as rank_id,
'Rolling up professional claims costs into AIP encounters' as field,
null as field_value


union all


select
118 as rank_id,
null as field,
null as field_value


union all


select
119 as rank_id,
'Place of Service Code atomic data quality:' as field,
null as field_value


union all


select
120 as rank_id,
'(valid pos codes) / (all pos codes) * 100' as field,
round(
(select count(*) from {{ ref('pos_all') }}
 where calculated_claim_type = 'professional' 
 and valid_place_of_service_code = 1) * 100.0 /
(select count(*) from {{ ref('pos_all') }}
 where calculated_claim_type = 'professional')
, 1) as field_value


union all


select
121 as rank_id,
'(claims with >= 1 usable pos code) / (inst claims) * 100' as field,
(select percent_of_professional_claims
 from {{ ref('usable_pos_code_histogram') }}
 where field = 'Claims with >=1 usable pos code') as field_value


union all


select
122 as rank_id,
'(claims with >= 2 usable pos code) / (inst claims) * 100' as field,
(select percent_of_professional_claims
 from {{ ref('usable_pos_code_histogram') }}
 where field = 'Claims with >=2 usable pos code') as field_value


union all


select
123 as rank_id,
'(claims with >= 3 usable pos code) / (inst claims) * 100' as field,
(select percent_of_professional_claims
 from {{ ref('usable_pos_code_histogram') }}
 where field = 'Claims with >=3 usable pos code') as field_value


union all


select
124 as rank_id,
'(claims with >= 4 usable pos code) / (inst claims) * 100' as field,
(select percent_of_professional_claims
 from {{ ref('usable_pos_code_histogram') }}
 where field = 'Claims with >=4 usable pos code') as field_value


union all


select
125 as rank_id,
'(claims with >= 5 usable pos code) / (inst claims) * 100' as field,
(select percent_of_professional_claims
 from {{ ref('usable_pos_code_histogram') }}
 where field = 'Claims with >=5 usable pos code') as field_value


union all


select
126 as rank_id,
'(claims with >= 6 usable pos code) / (inst claims) * 100' as field,
(select percent_of_professional_claims
 from {{ ref('usable_pos_code_histogram') }}
 where field = 'Claims with >=6 usable pos code') as field_value



union all


select
127 as rank_id,
'(claims with >= 7 usable pos code) / (inst claims) * 100' as field,
(select percent_of_professional_claims
 from {{ ref('usable_pos_code_histogram') }}
 where field = 'Claims with >=7 usable pos code') as field_value



union all


select
128 as rank_id,
'(claims with >= 8 usable pos code) / (inst claims) * 100' as field,
(select percent_of_professional_claims
 from {{ ref('usable_pos_code_histogram') }}
 where field = 'Claims with >=8 usable pos code') as field_value



union all


select
129 as rank_id,
'(claims with >= 9 usable pos code) / (inst claims) * 100' as field,
(select percent_of_professional_claims
 from {{ ref('usable_pos_code_histogram') }}
 where field = 'Claims with >=9 usable pos code') as field_value


union all


select
130 as rank_id,
'(claims with >= 10 usable pos code) / (inst claims) * 100' as field,
(select percent_of_professional_claims
 from {{ ref('usable_pos_code_histogram') }}
 where field = 'Claims with >=10 usable pos code') as field_value


union all


select
131 as rank_id,
null as field,
null as field_value


union all


select
132 as rank_id,
'Professional aip claims summary:' as field,
null as field_value


union all


select
133 as rank_id,
'total aip prof claims' as field,
(select field_value
 from {{ ref('all_prof_aip_claims_summary') }}
 where field = 'total aip prof claims') as field_value


union all


select
134 as rank_id,
'(aip prof claims with unusable patient_id) / (total aip prof claims) * 100' as field,
(select field_value
 from {{ ref('all_prof_aip_claims_summary') }}
 where field = '(aip prof claims with unusable patient_id) / (total aip prof claims) * 100') as field_value


union all


select
135 as rank_id,
'(aip prof claims with unusable merge dates) / (total aip prof claims) * 100' as field,
(select field_value
 from {{ ref('all_prof_aip_claims_summary') }}
 where field = '(aip prof claims with unusable merge dates) / (total aip prof claims) * 100') as field_value


union all


select
136 as rank_id,
'(usable aip prof claims) / (total aip prof claims) * 100' as field,
(select field_value
 from {{ ref('all_prof_aip_claims_summary') }}
 where field = '(usable aip prof claims) / (total aip prof claims) * 100') as field_value


union all


select
137 as rank_id,
null as field,
null as field_value


union all


select
138 as rank_id,
'Usable prof aip claims overlap summary' as field,
null as field_value


union all


select
139 as rank_id,
'Prof claims overlapping with one encounter' as field,
(select number_of_claims
 from {{ ref('prof_aip_overlap_summary') }}
 where field = 'Prof claims overlapping with one encounter') as field_value


union all


select
140 as rank_id,
'Prof claims overlapping with multiple encounters' as field,
(select number_of_claims
 from {{ ref('prof_aip_overlap_summary') }}
 where field = 'Prof claims overlapping with multiple encounters') as field_value


union all


select
141 as rank_id,
'Prof claims overlapping with no encounters' as field,
(select number_of_claims
 from {{ ref('prof_aip_overlap_summary') }}
 where field = 'Prof claims overlapping with no encounters') as field_value


union all


select
142 as rank_id,
'(Prof claims overlapping with one encounter) / (usable aip prof claims) * 100' as field,
(select percent_of_usable_aip_prof_claims
 from {{ ref('prof_aip_overlap_summary') }}
 where field = 'Prof claims overlapping with one encounter') as field_value


union all


select
143 as rank_id,
'(Prof claims overlapping with multiple encounters) / (usable aip prof claims) * 100' as field,
(select percent_of_usable_aip_prof_claims
 from {{ ref('prof_aip_overlap_summary') }}
 where field = 'Prof claims overlapping with multiple encounters') as field_value


union all


select
144 as rank_id,
'(Prof claims overlapping with no encounters) / (usable aip prof claims) * 100' as field,
(select percent_of_usable_aip_prof_claims
 from {{ ref('prof_aip_overlap_summary') }}
 where field = 'Prof claims overlapping with no encounters') as field_value


union all


select
145 as rank_id,
null as field,
null as field_value



union all


select
146 as rank_id,
'Summary of AIP encounters' as field,
null as field_value


union all


select
147 as rank_id,
'aip_encounters' as field,
(select field_value
 from {{ ref('aip_encounters_final_summary') }}
 where field = 'aip_encounters') as field_value


union all


select
148 as rank_id,
'(aip_encounters_with_dq_prob) / (aip_encounters) * 100' as field,
(select field_value
 from {{ ref('aip_encounters_final_summary') }}
 where field = '(aip_encounters_with_dq_prob) / (aip_encounters) * 100') as field_value


union all


select
149 as rank_id,
'(aip_encounters_with_unusable_ms_drg_code) / (aip_encounters) * 100' as field,
(select field_value
 from {{ ref('aip_encounters_final_summary') }}
 where field = '(aip_encounters_with_unusable_ms_drg_code) / (aip_encounters) * 100') as field_value


union all


select
150 as rank_id,
'(aip_encounters_with_unusable_apr_drg_code) / (aip_encounters) * 100' as field,
(select field_value
 from {{ ref('aip_encounters_final_summary') }}
 where field = '(aip_encounters_with_unusable_apr_drg_code) / (aip_encounters) * 100') as field_value


union all


select
151 as rank_id,
'(aip_encounters_with_unusable_dx1) / (aip_encounters) * 100' as field,
(select field_value
 from {{ ref('aip_encounters_final_summary') }}
 where field = '(aip_encounters_with_unusable_dx1) / (aip_encounters) * 100') as field_value


union all


select
152 as rank_id,
'(aip_encounters_with_unusable_atc) / (aip_encounters) * 100' as field,
(select field_value
 from {{ ref('aip_encounters_final_summary') }}
 where field = '(aip_encounters_with_unusable_atc) / (aip_encounters) * 100') as field_value


union all


select
153 as rank_id,
'(aip_encounters_with_unusable_asc) / (aip_encounters) * 100' as field,
(select field_value
 from {{ ref('aip_encounters_final_summary') }}
 where field = '(aip_encounters_with_unusable_asc) / (aip_encounters) * 100') as field_value


union all


select
154 as rank_id,
'(aip_encounters_with_unusable_ddc) / (aip_encounters) * 100' as field,
(select field_value
 from {{ ref('aip_encounters_final_summary') }}
 where field = '(aip_encounters_with_unusable_ddc) / (aip_encounters) * 100') as field_value


union all


select
155 as rank_id,
'(aip_encounters_with_unusable_facility_npi) / (aip_encounters) * 100' as field,
(select field_value
 from {{ ref('aip_encounters_final_summary') }}
 where field = '(aip_encounters_with_unusable_facility_npi) / (aip_encounters) * 100') as field_value


union all


select
156 as rank_id,
'(aip_encounters_with_unusable_rendering_npi) / (aip_encounters) * 100' as field,
(select field_value
 from {{ ref('aip_encounters_final_summary') }}
 where field = '(aip_encounters_with_unusable_rendering_npi) / (aip_encounters) * 100') as field_value


union all


select
157 as rank_id,
'(single_inst_claim_aip_encounters) / (aip_encounters) * 100' as field,
(select field_value
 from {{ ref('aip_encounters_final_summary') }}
 where field = '(single_inst_claim_aip_encounters) / (aip_encounters) * 100') as field_value


union all


select
158 as rank_id,
'(multiple_inst_claim_aip_encounters) / (aip_encounters) * 100' as field,
(select field_value
 from {{ ref('aip_encounters_final_summary') }}
 where field = '(multiple_inst_claim_aip_encounters) / (aip_encounters) * 100') as field_value


union all


select
159 as rank_id,
'(aip_encounters_with_prof_claims) / (aip_encounters) * 100' as field,
(select field_value
 from {{ ref('aip_encounters_final_summary') }}
 where field = '(aip_encounters_with_prof_claims) / (aip_encounters) * 100') as field_value


union all


select
160 as rank_id,
'(aip_encounters_without_prof_claims) / (aip_encounters) * 100' as field,
(select field_value
 from {{ ref('aip_encounters_final_summary') }}
 where field = '(aip_encounters_without_prof_claims) / (aip_encounters) * 100') as field_value


union all


select
161 as rank_id,
'(spend_from_prof_claims) / (total_spend_on_aip_encounters_with_prof_claims) * 100' as field,
(select field_value
 from {{ ref('aip_encounters_final_summary') }}
 where field = '(spend_from_prof_claims) / (total_spend_on_aip_encounters_with_prof_claims) * 100') as field_value


union all


select
162 as rank_id,
'(aip_encounters_with_death) / (aip_encounters) * 100' as field,
(select field_value
 from {{ ref('aip_encounters_final_summary') }}
 where field = '(aip_encounters_with_death) / (aip_encounters) * 100') as field_value


union all


select
163 as rank_id,
'average_los' as field,
(select field_value
 from {{ ref('aip_encounters_final_summary') }}
 where field = 'average_los') as field_value


union all


select
164 as rank_id,
'average_total_paid_amount' as field,
(select field_value
 from {{ ref('aip_encounters_final_summary') }}
 where field = 'average_total_paid_amount') as field_value
