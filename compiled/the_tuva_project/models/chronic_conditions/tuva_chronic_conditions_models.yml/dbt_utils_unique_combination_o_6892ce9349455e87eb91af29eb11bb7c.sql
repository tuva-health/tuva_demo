





with validation_errors as (

    select
        patient_id, condition
    from tuva_synthetic.chronic_conditions.tuva_chronic_conditions_long
    group by patient_id, condition
    having count(*) > 1

)

select *
from validation_errors


