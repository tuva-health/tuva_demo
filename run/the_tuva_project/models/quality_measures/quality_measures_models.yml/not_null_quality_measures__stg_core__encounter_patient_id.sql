select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



with __dbt__cte__quality_measures__stg_core__encounter as (


select
      patient_id
    , encounter_id
    , encounter_type
    , length_of_stay
    , encounter_start_date
    , encounter_end_date
    , '2024-10-04 19:08:24.316902+00:00' as tuva_last_run
from tuva_synthetic.core.encounter


) select patient_id
from __dbt__cte__quality_measures__stg_core__encounter
where patient_id is null



      
    ) dbt_internal_test