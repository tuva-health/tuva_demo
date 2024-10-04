
  
    

        create or replace transient table tuva_synthetic.data_quality.data_quality_for_pbi
         as
        (

SELECT
    data_source,
    field_name,
    table_name,
    'CLINICAL' AS claim_type,
    bucket_name,
    field_value,
    drill_down_key,
    drill_down_value,
    invalid_reason,
    summary_sk,
    frequency,
	'2024-10-04 19:08:24.316902+00:00' as tuva_last_run
FROM tuva_synthetic.data_quality.data_quality_clinical_for_pbi
        );
      
  