

SELECT *,
    patient_id || '|' || data_source as patient_data_source_key
FROM tuva_synthetic.core.patient