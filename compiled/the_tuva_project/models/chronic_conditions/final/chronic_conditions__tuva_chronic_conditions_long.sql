

with  __dbt__cte__tuva_chronic_conditions__stg_core__condition as (


select 
      patient_id
    , normalized_code
    , recorded_date
    , '2024-10-04 19:11:18.274664+00:00' as tuva_last_run
from tuva_synthetic.core.condition
), all_conditions as (
select 
  patient_id,
  normalized_code,
  recorded_date
    from __dbt__cte__tuva_chronic_conditions__stg_core__condition
),


conditions_with_first_and_last_diagnosis_date as (
select 
  patient_id,
  normalized_code as icd_10_cm,
  min(recorded_date) as first_diagnosis_date,
  max(recorded_date) as last_diagnosis_date
from all_conditions
group by patient_id, normalized_code

)


select
  aa.patient_id,
  bb.concept_name as condition,
  min(first_diagnosis_date) as first_diagnosis_date,
  max(last_diagnosis_date) as last_diagnosis_date,
  '2024-10-04 19:11:18.274664+00:00' as tuva_last_run
from conditions_with_first_and_last_diagnosis_date aa
inner join tuva_synthetic.clinical_concept_library.value_set_member_relevant_fields bb
on aa.icd_10_cm = bb.code
group by aa.patient_id, bb.concept_name