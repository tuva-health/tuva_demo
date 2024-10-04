
  create or replace   view tuva_synthetic.input_layer.encounter
  
   as (
    
    select
    cast(null as TEXT ) as encounter_id
    , cast(null as TEXT ) as patient_id
    , cast(null as TEXT ) as encounter_type
    , cast(null as date) as encounter_start_date
    , cast(null as date) as encounter_end_date
    , cast(null as INT ) as length_of_stay
    , cast(null as TEXT ) as admit_source_code
    , cast(null as TEXT ) as admit_source_description
    , cast(null as TEXT ) as admit_type_code
    , cast(null as TEXT ) as admit_type_description
    , cast(null as TEXT ) as discharge_disposition_code
    , cast(null as TEXT ) as discharge_disposition_description
    , cast(null as TEXT ) as attending_provider_id
    , cast(null as TEXT ) as attending_provider_name
    , cast(null as TEXT ) as facility_id
    , cast(null as TEXT ) as facility_name
    , cast(null as TEXT ) as primary_diagnosis_code_type
    , cast(null as TEXT ) as primary_diagnosis_code
    , cast(null as TEXT ) as primary_diagnosis_description
    , cast(null as TEXT ) as ms_drg_code
    , cast(null as TEXT ) as ms_drg_description
    , cast(null as TEXT ) as apr_drg_code
    , cast(null as TEXT ) as apr_drg_description
    , cast(null as FLOAT ) as paid_amount
    , cast(null as FLOAT ) as allowed_amount
    , cast(null as FLOAT ) as charge_amount
    , cast(null as TEXT ) as data_source
    , cast(null as TIMESTAMP ) as tuva_last_run
    limit 0

  );

