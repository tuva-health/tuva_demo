
  create or replace   view tuva_synthetic.input_layer.procedure
  
   as (
    
    select
     cast(null as TEXT ) as procedure_id
    , cast(null as TEXT ) as patient_id
    , cast(null as TEXT ) as encounter_id
    , cast(null as TEXT ) as claim_id
    , cast(null as date) as procedure_date
    , cast(null as TEXT ) as source_code_type
    , cast(null as TEXT ) as source_code
    , cast(null as TEXT ) as source_description
    , cast(null as TEXT ) as normalized_code_type
    , cast(null as TEXT ) as normalized_code
    , cast(null as TEXT ) as normalized_description
    , cast(null as TEXT ) as modifier_1
    , cast(null as TEXT ) as modifier_2
    , cast(null as TEXT ) as modifier_3
    , cast(null as TEXT ) as modifier_4
    , cast(null as TEXT ) as modifier_5
    , cast(null as TEXT ) as practitioner_id
    , cast(null as TEXT ) as data_source
    , cast(null as TIMESTAMP ) as tuva_last_run
    limit 0

  );

