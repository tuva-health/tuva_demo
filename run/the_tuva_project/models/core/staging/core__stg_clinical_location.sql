
  create or replace   view tuva_synthetic.core._stg_clinical_location
  
   as (
    

select
    cast(location_id as TEXT ) as location_id
    , cast(npi as TEXT ) as npi
    , cast(name as TEXT ) as name
    , cast(facility_type as TEXT ) as facility_type
    , cast(parent_organization as TEXT ) as parent_organization
    , cast(address as TEXT ) as address
    , cast(city as TEXT ) as city
    , cast(state as TEXT ) as state
    , cast(zip_code as TEXT ) as zip_code
    , cast(latitude as FLOAT ) as latitude
    , cast(longitude as FLOAT ) as longitude
    , cast(data_source as TEXT ) as data_source
    , cast('2024-10-04 19:08:24.316902+00:00' as TIMESTAMP ) as tuva_last_run
from tuva_synthetic.input_layer.location
  );

