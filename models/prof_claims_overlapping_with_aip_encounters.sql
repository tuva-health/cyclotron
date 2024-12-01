

select
  aa.claim_id,
  aa.patient_id,
  aa.paid_amount,
  aa.usable_patient_id,
  aa.merge_start_date,
  aa.merge_end_date,
  aa.usable_merge_dates,
  aa.usable_prof_claim,

  bb.encounter_id


from {{ ref('all_prof_aip_claims') }} aa
left join {{ ref('aip_encounters_institutional_definition') }} bb
on aa.patient_id = bb.patient_id
and
(aa.merge_start_date between bb.encounter_start_date and bb.encounter_end_date
 or
 aa.merge_end_date between bb.encounter_start_date and bb.encounter_end_date
 or
 bb.encounter_start_date between aa.merge_start_date and aa.merge_end_date
 or
bb.encounter_end_date between aa.merge_start_date and aa.merge_end_date
)

where aa.usable_prof_claim = 1
