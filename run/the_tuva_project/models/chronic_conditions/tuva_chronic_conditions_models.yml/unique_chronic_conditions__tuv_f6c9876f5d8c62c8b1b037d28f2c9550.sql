select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    patient_id as unique_field,
    count(*) as n_records

from tuva_synthetic.chronic_conditions.tuva_chronic_conditions_wide
where patient_id is not null
group by patient_id
having count(*) > 1



      
    ) dbt_internal_test