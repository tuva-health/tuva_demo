
  
    

        create or replace transient table tuva_synthetic.data_quality.mart_review__tuva_chronic_conditions
         as
        (

with cte as (
    select distinct
        patient_id
    from tuva_synthetic.chronic_conditions.tuva_chronic_conditions_long
)

, patientxwalk as (
    select distinct
        patient_id
      , data_source
    from tuva_synthetic.core.patient
)

, result as (
    select
        l.patient_id
      , p.data_source
      , l.condition
    from tuva_synthetic.chronic_conditions.tuva_chronic_conditions_long as l
    inner join patientxwalk as p
      on l.patient_id = p.patient_id

    union all

    select
        p.patient_id
      , p.data_source
      , 'No Chronic Conditions' as condition
    from tuva_synthetic.core.patient as p
    left join cte
      on p.patient_id = cte.patient_id
    where cte.patient_id is null
)

select *
   , patient_id || '|' || data_source as patient_source_key
    , '2024-10-04 19:08:24.316902+00:00' as tuva_last_run
from result
        );
      
  