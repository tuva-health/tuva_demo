select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    observation_id as unique_field,
    count(*) as n_records

from tuva_synthetic.core.observation
where observation_id is not null
group by observation_id
having count(*) > 1



      
    ) dbt_internal_test