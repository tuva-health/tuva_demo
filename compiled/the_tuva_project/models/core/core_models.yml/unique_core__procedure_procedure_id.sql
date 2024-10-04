
    
    

select
    procedure_id as unique_field,
    count(*) as n_records

from tuva_synthetic.core.procedure
where procedure_id is not null
group by procedure_id
having count(*) > 1


