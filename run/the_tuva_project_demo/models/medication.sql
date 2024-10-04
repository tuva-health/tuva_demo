
  create or replace   view tuva_synthetic.input_layer.medication
  
   as (
    
    select
    cast(null as TEXT ) as medication_id
    , cast(null as TEXT ) as patient_id
    , cast(null as TEXT ) as encounter_id
    , cast(null as date) as dispensing_date
    , cast(null as date) as prescribing_date
    , cast(null as TEXT ) as source_code_type
    , cast(null as TEXT ) as source_code
    , cast(null as TEXT ) as source_description
    , cast(null as TEXT ) as ndc_code
    , cast(null as TEXT ) as ndc_description
    , cast(null as TEXT ) as rxnorm_code
    , cast(null as TEXT ) as rxnorm_description
    , cast(null as TEXT ) as atc_code
    , cast(null as TEXT ) as atc_description
    , cast(null as TEXT ) as route
    , cast(null as TEXT ) as strength
    , cast(null as INT ) as quantity
    , cast(null as TEXT ) as quantity_unit
    , cast(null as INT ) as days_supply
    , cast(null as TEXT ) as practitioner_id
    , cast(null as TEXT ) as data_source
    , cast(null as TIMESTAMP ) as tuva_last_run
    limit 0

  );

