
  create or replace   view tuva_synthetic.input_layer.location
  
   as (
    
    select
    cast(null as TEXT ) as location_id
    , cast(null as TEXT ) as npi
    , cast(null as TEXT ) as name
    , cast(null as TEXT ) as facility_type
    , cast(null as TEXT ) as parent_organization
    , cast(null as TEXT ) as address
    , cast(null as TEXT ) as city
    , cast(null as TEXT ) as state
    , cast(null as TEXT ) as zip_code
    , cast(null as FLOAT ) as latitude
    , cast(null as FLOAT ) as longitude
    , cast(null as TEXT ) as data_source
    , cast(null as TIMESTAMP ) as tuva_last_run
    limit 0

  );

