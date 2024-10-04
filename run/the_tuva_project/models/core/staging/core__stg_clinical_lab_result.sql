
  create or replace   view tuva_synthetic.core._stg_clinical_lab_result
  
   as (
    

select
    cast(lab_result_id as TEXT ) as lab_result_id
    , cast(patient_id as TEXT ) as patient_id
    , cast(encounter_id as TEXT ) as encounter_id
    , cast(accession_number as TEXT ) as accession_number
    , cast(source_code_type as TEXT ) as source_code_type
    , cast(source_code as TEXT ) as source_code
    , cast(source_description as TEXT ) as source_description
    , cast(source_component as TEXT ) as source_component
    , cast(normalized_code_type as TEXT ) as normalized_code_type
    , cast(normalized_code as TEXT ) as normalized_code
    , cast(normalized_description as TEXT ) as normalized_description
    , cast(normalized_component as TEXT ) as normalized_component
    , cast(status as TEXT ) as status
    , cast(result as TEXT ) as result
    , try_cast( result_date as date ) as result_date
    , try_cast( collection_date as date ) as collection_date
    , cast(source_units as TEXT ) as source_units
    , cast(normalized_units as TEXT ) as normalized_units
    , cast(source_reference_range_low as TEXT ) as source_reference_range_low
    , cast(source_reference_range_high as TEXT ) as source_reference_range_high
    , cast(normalized_reference_range_low as TEXT ) as normalized_reference_range_low
    , cast(normalized_reference_range_high as TEXT ) as normalized_reference_range_high
    , cast(source_abnormal_flag as TEXT ) as source_abnormal_flag
    , cast(normalized_abnormal_flag as TEXT ) as normalized_abnormal_flag
    , cast(specimen as TEXT ) as specimen
    , cast(ordering_practitioner_id as TEXT ) as ordering_practitioner_id
    , cast(data_source as TEXT ) as data_source
    , cast('2024-10-04 19:08:24.316902+00:00' as TIMESTAMP ) as tuva_last_run
from tuva_synthetic.input_layer.lab_result
  );

