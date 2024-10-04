
    
    

select
    icd9 as unique_field,
    count(*) as n_records

from tuva_synthetic.ed_classification._value_set_johnston_icd9
where icd9 is not null
group by icd9
having count(*) > 1


