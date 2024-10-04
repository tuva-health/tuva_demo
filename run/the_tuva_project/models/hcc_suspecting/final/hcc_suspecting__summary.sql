
  
    

        create or replace transient table tuva_synthetic.hcc_suspecting.summary
         as
        (

with  __dbt__cte__hcc_suspecting__stg_core__patient as (

select
      patient_id
    , sex
    , birth_date
    , death_date
from tuva_synthetic.core.patient
), patients as (

    select
          patient_id
        , sex
        , birth_date
        
            , floor(datediff(
        hour,
        birth_date,
        current_date
        ) / 8766.0) as age
        
    from __dbt__cte__hcc_suspecting__stg_core__patient
    where death_date is null

)

, suspecting_list as (

      select
          patient_id
        , count(*) as gaps
    from tuva_synthetic.hcc_suspecting.list
    group by patient_id

)

, joined as (

    select
          patients.patient_id
        , patients.sex
        , patients.birth_date
        , patients.age
        , suspecting_list.gaps
    from patients
         inner join suspecting_list
         on patients.patient_id = suspecting_list.patient_id

)

, add_data_types as (

    select
          cast(patient_id as TEXT) as patient_id
        , cast(sex as TEXT) as patient_sex
        , cast(birth_date as date) as patient_birth_date
        , cast(age as integer) as patient_age
        , cast(gaps as integer) as suspecting_gaps
    from joined

)

select
      patient_id
    , patient_sex
    , patient_birth_date
    , patient_age
    , suspecting_gaps
    , '2024-10-04 19:08:24.316902+00:00' as tuva_last_run
from add_data_types
        );
      
  