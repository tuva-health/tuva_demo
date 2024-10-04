
  create or replace   view tuva_synthetic.core._stg_clinical_patient
  
   as (
    

with tuva_last_run as(
    select
       cast('2024-10-04 19:08:24.316902+00:00' as TIMESTAMP ) as tuva_last_run_datetime
       , cast(substring('2024-10-04 19:08:24.316902+00:00',1,10) as date ) as tuva_last_run_date
)
SELECT
    cast(patient_id as TEXT ) as patient_id
    , cast(first_name as TEXT ) as first_name
    , cast(last_name as TEXT ) as last_name
    , cast(sex as TEXT ) as sex
    , cast(race as TEXT ) as race
    , try_cast( birth_date as date ) as birth_date
    , try_cast( death_date as date ) as death_date
    , cast(death_flag as INT ) as death_flag
    , cast(social_security_number as TEXT ) as social_security_number
    , cast(address as TEXT ) as address
    , cast(city as TEXT ) as city
    , cast(state as TEXT ) as state
    , cast(zip_code as TEXT ) as zip_code
    , cast(county as TEXT ) as county
    , cast(latitude as FLOAT ) as latitude
    , cast(longitude as FLOAT ) as longitude
    , cast(data_source as TEXT ) as data_source
    , cast(floor(datediff(
        hour,
        birth_date,
        tuva_last_run_date
        ) / 8760.0) as INT ) as age
    , cast(
        CASE
            WHEN cast(floor(datediff(
        hour,
        birth_date,
        tuva_last_run_date
        ) / 8760.0) as INT ) < 10 THEN '0-9'
            WHEN cast(floor(datediff(
        hour,
        birth_date,
        tuva_last_run_date
        ) / 8760.0) as INT ) < 20 THEN '10-19'
            WHEN cast(floor(datediff(
        hour,
        birth_date,
        tuva_last_run_date
        ) / 8760.0) as INT ) < 30 THEN '20-29'
            WHEN cast(floor(datediff(
        hour,
        birth_date,
        tuva_last_run_date
        ) / 8760.0) as INT ) < 40 THEN '30-39'
            WHEN cast(floor(datediff(
        hour,
        birth_date,
        tuva_last_run_date
        ) / 8760.0) as INT ) < 50 THEN '40-49'
            WHEN cast(floor(datediff(
        hour,
        birth_date,
        tuva_last_run_date
        ) / 8760.0) as INT ) < 60 THEN '50-59'
            WHEN cast(floor(datediff(
        hour,
        birth_date,
        tuva_last_run_date
        ) / 8760.0) as INT ) < 70 THEN '60-69'
            WHEN cast(floor(datediff(
        hour,
        birth_date,
        tuva_last_run_date
        ) / 8760.0) as INT ) < 80 THEN '70-79'
            WHEN cast(floor(datediff(
        hour,
        birth_date,
        tuva_last_run_date
        ) / 8760.0) as INT ) < 90 THEN '80-89'
            ELSE '90+'
        END as TEXT
    ) AS age_group
    , tuva_last_run_datetime as tuva_last_run
FROM tuva_synthetic.input_layer.patient
cross join tuva_last_run
  );

