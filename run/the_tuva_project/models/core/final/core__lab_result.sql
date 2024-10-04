
  
    

        create or replace transient table tuva_synthetic.core.lab_result
         as
        (




select
      labs.lab_result_id
    , labs.patient_id
    , labs.encounter_id
    , labs.accession_number
    , labs.source_code_type
    , labs.source_code
    , labs.source_description
    , labs.source_component
    , case
        when labs.normalized_code_type is not null then labs.normalized_code_type
        when loinc.loinc is not null then 'loinc'
        when snomed_ct.snomed_ct is not null then 'snomed-ct'
        else null end as normalized_code_type
    , coalesce(
        labs.normalized_code
        , loinc.loinc
        , snomed_ct.snomed_ct
        ) as normalized_code
    , coalesce(
        labs.normalized_description
        , loinc.long_common_name
        , snomed_ct.description
        ) normalized_description
    , case when coalesce(labs.normalized_code, labs.normalized_description) is not null then 'manual'
         when coalesce(loinc.loinc,snomed_ct.snomed_ct) is not null then 'automatic'
         end as mapping_method
    , labs.normalized_component
    , labs.status
    , labs.result
    , labs.result_date
    , labs.collection_date
    , labs.source_units
    , labs.normalized_units
    , labs.source_reference_range_low
    , labs.source_reference_range_high
    , labs.normalized_reference_range_low
    , labs.normalized_reference_range_high
    , labs.source_abnormal_flag
    , labs.normalized_abnormal_flag
    , labs.specimen
    , labs.ordering_practitioner_id
    , labs.data_source
    , labs.tuva_last_run
From tuva_synthetic.core._stg_clinical_lab_result as labs
left join tuva_synthetic.terminology.loinc loinc
    on labs.source_code_type = 'loinc'
        and labs.source_code = loinc.loinc
left join tuva_synthetic.terminology.snomed_ct snomed_ct
    on labs.source_code_type = 'snomed-ct'
        and labs.source_code = snomed_ct.snomed_ct

 
        );
      
  