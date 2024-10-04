

SELECT
    data_source,
	source_date,
	table_name,
	drill_down_key,
	drill_down_value,
	'clinical' as claim_type,
	field_name,
	bucket_name,
	invalid_reason,
	field_value,
	summary_sk,
	'2024-10-04 19:11:18.274664+00:00' as tuva_last_run
FROM tuva_synthetic.data_quality.data_quality_clinical_detail