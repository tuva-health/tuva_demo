select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select patient_id
from tuva_synthetic.chronic_conditions.tuva_chronic_conditions_wide
where patient_id is null



      
    ) dbt_internal_test