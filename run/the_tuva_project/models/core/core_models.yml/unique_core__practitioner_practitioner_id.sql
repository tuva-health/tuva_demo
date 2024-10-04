select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    practitioner_id as unique_field,
    count(*) as n_records

from tuva_synthetic.core.practitioner
where practitioner_id is not null
group by practitioner_id
having count(*) > 1



      
    ) dbt_internal_test