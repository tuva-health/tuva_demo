
  
    

        create or replace transient table tuva_synthetic.core.condition
         as
        (


with all_conditions as (
   select *
from tuva_synthetic.core._stg_clinical_condition


)




select
    all_conditions.condition_id
  , all_conditions.patient_id
  , all_conditions.encounter_id
  , all_conditions.claim_id
  , all_conditions.recorded_date
  , all_conditions.onset_date
  , all_conditions.resolved_date
  , all_conditions.status
  , all_conditions.condition_type
  , all_conditions.source_code_type
  , all_conditions.source_code
  , all_conditions.source_description
  , case
        when all_conditions.normalized_code_type is not null then all_conditions.normalized_code_type
        when icd10.icd_10_cm is not null then 'icd-10-cm'
        when icd9.icd_9_cm is not null then 'icd-9-cm'
        when snomed_ct.snomed_ct is not null then 'snomed-ct'
        else null end as normalized_code_type
  , coalesce(
        all_conditions.normalized_code
      , icd10.icd_10_cm
      , icd9.icd_9_cm
      , snomed_ct.snomed_ct) as normalized_code
  , coalesce(
        all_conditions.normalized_description
      , icd10.short_description
      , icd9.short_description
      , snomed_ct.description) as normalized_description
  , case when coalesce(all_conditions.normalized_code, all_conditions.normalized_description) is not null then 'manual'
         when coalesce(icd10.icd_10_cm,icd9.icd_9_cm, snomed_ct.snomed_ct) is not null then 'automatic'
         end as mapping_method
  , all_conditions.condition_rank
  , all_conditions.present_on_admit_code
  , all_conditions.present_on_admit_description
  , all_conditions.data_source
  , all_conditions.tuva_last_run
from
all_conditions
left join tuva_synthetic.terminology.icd_10_cm icd10
    on all_conditions.source_code_type = 'icd-10-cm'
        and replace(all_conditions.source_code,'.','') = icd10.icd_10_cm
left join tuva_synthetic.terminology.icd_9_cm icd9
    on all_conditions.source_code_type = 'icd-9-cm'
        and replace(all_conditions.source_code,'.','') = icd9.icd_9_cm
left join tuva_synthetic.terminology.snomed_ct snomed_ct
    on all_conditions.source_code_type = 'snomed-ct'
        and all_conditions.source_code = snomed_ct.snomed_ct





        );
      
  