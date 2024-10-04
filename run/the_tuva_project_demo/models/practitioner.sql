
  create or replace   view tuva_synthetic.input_layer.practitioner
  
   as (
    
    select
     cast(null as TEXT ) as practitioner_id
    , cast(null as TEXT ) as npi
    , cast(null as TEXT ) as first_name
    , cast(null as TEXT ) as last_name
    , cast(null as TEXT ) as practice_affiliation
    , cast(null as TEXT ) as specialty
    , cast(null as TEXT ) as sub_specialty
    , cast(null as TEXT ) as data_source
    , cast(null as TIMESTAMP ) as tuva_last_run
    limit 0

  );

