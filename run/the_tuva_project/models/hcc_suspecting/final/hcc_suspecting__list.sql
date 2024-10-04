
  
    

        create or replace transient table tuva_synthetic.hcc_suspecting.list
         as
        (

with hcc_history_suspects as (

    select distinct
          patient_id
        , data_source
        , hcc_code
        , hcc_description
        , reason
        , contributing_factor
        , suspect_date
    from tuva_synthetic.hcc_suspecting._int_patient_hcc_history
    
        where (current_year_billed = false
            or current_year_billed is null)
    

)

, comorbidity_suspects as (

    select distinct
          patient_id
        , data_source
        , hcc_code
        , hcc_description
        , reason
        , contributing_factor
        , suspect_date
    from tuva_synthetic.hcc_suspecting._int_comorbidity_suspects
    
        where (current_year_billed = false
            or current_year_billed is null)
    

)

, observation_suspects as (

    select distinct
          patient_id
        , data_source
        , hcc_code
        , hcc_description
        , reason
        , contributing_factor
        , suspect_date
    from tuva_synthetic.hcc_suspecting._int_observation_suspects
    
        where (current_year_billed = false
            or current_year_billed is null)
    

)

, lab_suspects as (

    select distinct
          patient_id
        , data_source
        , hcc_code
        , hcc_description
        , reason
        , contributing_factor
        , suspect_date
    from tuva_synthetic.hcc_suspecting._int_lab_suspects
    
        where (current_year_billed = false
            or current_year_billed is null)
    

)

, medication_suspects as (

    select distinct
          patient_id
        , data_source
        , hcc_code
        , hcc_description
        , reason
        , contributing_factor
        , suspect_date
    from tuva_synthetic.hcc_suspecting._int_medication_suspects
    
        where (current_year_billed = false
            or current_year_billed is null)
    

)

, unioned as (

    select * from hcc_history_suspects
    union all
    select * from comorbidity_suspects
    union all
    select * from observation_suspects
    union all
    select * from lab_suspects
    union all
    select * from medication_suspects

)

, add_data_types as (

    select
          cast(patient_id as TEXT) as patient_id
        , cast(data_source as TEXT) as data_source
        , cast(hcc_code as TEXT) as hcc_code
        , cast(hcc_description as TEXT) as hcc_description
        , cast(reason as TEXT) as reason
        , cast(contributing_factor as TEXT) as contributing_factor
        , cast(suspect_date as date) as suspect_date
    from unioned

)

select
      patient_id
    , data_source
    , hcc_code
    , hcc_description
    , reason
    , contributing_factor
    , suspect_date
    , '2024-10-04 19:08:24.316902+00:00' as tuva_last_run
from add_data_types
        );
      
  