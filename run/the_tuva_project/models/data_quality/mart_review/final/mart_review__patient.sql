
  
    

        create or replace transient table tuva_synthetic.data_quality.mart_review__patient
         as
        (

SELECT *,
    patient_id || '|' || data_source as patient_data_source_key
FROM tuva_synthetic.core.patient
        );
      
  