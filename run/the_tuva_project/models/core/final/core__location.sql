
  
    

        create or replace transient table tuva_synthetic.core.location
         as
        (

select * from tuva_synthetic.core._stg_clinical_location


        );
      
  