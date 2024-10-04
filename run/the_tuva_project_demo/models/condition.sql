
  create or replace   view tuva_synthetic.input_layer.condition
  
   as (
    
    select
     cast(null as TEXT ) as condition_id
    , cast(null as TEXT ) as patient_id
    , cast(null as TEXT ) as encounter_id
    , cast(null as TEXT ) as claim_id
    , cast(null as date) as recorded_date
    , cast(null as date) as onset_date
    , cast(null as date) as resolved_date
    , cast(null as TEXT ) as status
    , cast(null as TEXT ) as condition_type
    , cast(null as TEXT ) as source_code_type
    , cast(null as TEXT ) as source_code
    , cast(null as TEXT ) as source_description
    , cast(null as TEXT ) as normalized_code_type
    , cast(null as TEXT ) as normalized_code
    , cast(null as TEXT ) as normalized_description
    , cast(null as INT ) as condition_rank
    , cast(null as TEXT ) as present_on_admit_code
    , cast(null as TEXT ) as present_on_admit_description
    , cast(null as TEXT ) as data_source
    , cast(null as TIMESTAMP ) as tuva_last_run
    limit 0

  );

