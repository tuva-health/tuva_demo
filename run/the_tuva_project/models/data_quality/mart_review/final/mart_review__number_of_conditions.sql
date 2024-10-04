
  
    

        create or replace transient table tuva_synthetic.data_quality.mart_review__number_of_conditions
         as
        (

with xwalk as (
    select distinct patient_id, data_source
    from tuva_synthetic.core.patient
),
cte as (
    select l.patient_id,
           x.data_source,
           count(*) as numofconditions
    from tuva_synthetic.chronic_conditions.tuva_chronic_conditions_long l
    left join xwalk x on l.patient_id = x.patient_id
    group by l.patient_id, x.data_source
)
select p.patient_id,
       p.data_source,
        p.patient_id || '|' || p.data_source as patient_source_key,
       coalesce(cte.numofconditions, 0) as numofconditions
    , '2024-10-04 19:08:24.316902+00:00' as tuva_last_run
from tuva_synthetic.core.patient p
left join cte on p.patient_id = cte.patient_id and p.data_source = cte.data_source
        );
      
  