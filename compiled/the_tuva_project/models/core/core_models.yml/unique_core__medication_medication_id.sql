
    
    

select
    medication_id as unique_field,
    count(*) as n_records

from tuva_synthetic.core.medication
where medication_id is not null
group by medication_id
having count(*) > 1


