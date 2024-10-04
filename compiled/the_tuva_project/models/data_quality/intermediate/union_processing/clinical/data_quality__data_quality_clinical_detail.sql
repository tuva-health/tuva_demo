

WITH CTE as (
SELECT * FROM tuva_synthetic.data_quality.condition_claim_id

union all

SELECT * FROM tuva_synthetic.data_quality.condition_condition_id

union all

SELECT * FROM tuva_synthetic.data_quality.condition_condition_rank

union all

SELECT * FROM tuva_synthetic.data_quality.condition_condition_type

union all

SELECT * FROM tuva_synthetic.data_quality.condition_data_source

union all

SELECT * FROM tuva_synthetic.data_quality.condition_encounter_id

union all

SELECT * FROM tuva_synthetic.data_quality.condition_normalized_code_type

union all

SELECT * FROM tuva_synthetic.data_quality.condition_normalized_code

union all

SELECT * FROM tuva_synthetic.data_quality.condition_normalized_description

union all

SELECT * FROM tuva_synthetic.data_quality.condition_onset_date

union all

SELECT * FROM tuva_synthetic.data_quality.condition_patient_id

union all

SELECT * FROM tuva_synthetic.data_quality.condition_present_on_admit_code

union all

SELECT * FROM tuva_synthetic.data_quality.condition_present_on_admit_description

union all

SELECT * FROM tuva_synthetic.data_quality.condition_recorded_date

union all

SELECT * FROM tuva_synthetic.data_quality.condition_resolved_date

union all

SELECT * FROM tuva_synthetic.data_quality.condition_source_code

union all

SELECT * FROM tuva_synthetic.data_quality.condition_source_code_type

union all

SELECT * FROM tuva_synthetic.data_quality.condition_source_description

union all

SELECT * FROM tuva_synthetic.data_quality.condition_status

union all

SELECT * FROM tuva_synthetic.data_quality.encounter_admit_source_code

union all

SELECT * FROM tuva_synthetic.data_quality.encounter_admit_source_description

union all

SELECT * FROM tuva_synthetic.data_quality.encounter_admit_type_code

union all

SELECT * FROM tuva_synthetic.data_quality.encounter_admit_type_description

union all

SELECT * FROM tuva_synthetic.data_quality.encounter_allowed_amount

union all

SELECT * FROM tuva_synthetic.data_quality.encounter_apr_drg_code

union all

SELECT * FROM tuva_synthetic.data_quality.encounter_apr_drg_description

union all

SELECT * FROM tuva_synthetic.data_quality.encounter_attending_provider_id

union all

SELECT * FROM tuva_synthetic.data_quality.encounter_charge_amount

union all

SELECT * FROM tuva_synthetic.data_quality.encounter_data_source

union all

SELECT * FROM tuva_synthetic.data_quality.encounter_discharge_disposition_code

union all

SELECT * FROM tuva_synthetic.data_quality.encounter_discharge_disposition_description

union all

SELECT * FROM tuva_synthetic.data_quality.encounter_encounter_end_date

union all

SELECT * FROM tuva_synthetic.data_quality.encounter_encounter_id

union all

SELECT * FROM tuva_synthetic.data_quality.encounter_encounter_start_date

union all

SELECT * FROM tuva_synthetic.data_quality.encounter_encounter_type

union all

SELECT * FROM tuva_synthetic.data_quality.encounter_facility_id

union all

SELECT * FROM tuva_synthetic.data_quality.encounter_length_of_stay

union all

SELECT * FROM tuva_synthetic.data_quality.encounter_ms_drg_code

union all

SELECT * FROM tuva_synthetic.data_quality.encounter_ms_drg_description

union all

SELECT * FROM tuva_synthetic.data_quality.encounter_paid_amount

union all

SELECT * FROM tuva_synthetic.data_quality.encounter_patient_id

union all

SELECT * FROM tuva_synthetic.data_quality.encounter_primary_diagnosis_code

union all

SELECT * FROM tuva_synthetic.data_quality.encounter_primary_diagnosis_code_type

union all

SELECT * FROM tuva_synthetic.data_quality.encounter_primary_diagnosis_description

union all

SELECT * FROM tuva_synthetic.data_quality.practitioner_practice_affiliation

union all

SELECT * FROM tuva_synthetic.data_quality.practitioner_sub_specialty

union all

SELECT * FROM tuva_synthetic.data_quality.practitioner_last_name

union all

SELECT * FROM tuva_synthetic.data_quality.practitioner_practitioner_id

union all

SELECT * FROM tuva_synthetic.data_quality.practitioner_data_source

union all

SELECT * FROM tuva_synthetic.data_quality.practitioner_npi

union all

SELECT * FROM tuva_synthetic.data_quality.practitioner_first_name

union all

SELECT * FROM tuva_synthetic.data_quality.practitioner_specialty

union all

SELECT * FROM tuva_synthetic.data_quality.location_parent_organization

union all

SELECT * FROM tuva_synthetic.data_quality.location_latitude

union all

SELECT * FROM tuva_synthetic.data_quality.location_facility_type

union all

SELECT * FROM tuva_synthetic.data_quality.location_zip_code

union all

SELECT * FROM tuva_synthetic.data_quality.location_data_source

union all

SELECT * FROM tuva_synthetic.data_quality.location_city

union all

SELECT * FROM tuva_synthetic.data_quality.location_npi

union all

SELECT * FROM tuva_synthetic.data_quality.location_location_id

union all

SELECT * FROM tuva_synthetic.data_quality.location_longitude

union all

SELECT * FROM tuva_synthetic.data_quality.location_address

union all

SELECT * FROM tuva_synthetic.data_quality.location_state

union all

SELECT * FROM tuva_synthetic.data_quality.location_name

union all

SELECT * FROM tuva_synthetic.data_quality.procedure_normalized_code_type

union all

SELECT * FROM tuva_synthetic.data_quality.procedure_normalized_description

union all

SELECT * FROM tuva_synthetic.data_quality.procedure_procedure_id

union all

SELECT * FROM tuva_synthetic.data_quality.procedure_claim_id

union all

SELECT * FROM tuva_synthetic.data_quality.procedure_source_code

union all

SELECT * FROM tuva_synthetic.data_quality.procedure_source_code_type

union all

SELECT * FROM tuva_synthetic.data_quality.procedure_source_description

union all

SELECT * FROM tuva_synthetic.data_quality.procedure_practitioner_id

union all

SELECT * FROM tuva_synthetic.data_quality.procedure_data_source

union all

SELECT * FROM tuva_synthetic.data_quality.procedure_patient_id

union all

SELECT * FROM tuva_synthetic.data_quality.procedure_procedure_date

union all

SELECT * FROM tuva_synthetic.data_quality.procedure_encounter_id

union all

SELECT * FROM tuva_synthetic.data_quality.procedure_modifier_5

union all

SELECT * FROM tuva_synthetic.data_quality.procedure_modifier_4

union all

SELECT * FROM tuva_synthetic.data_quality.procedure_normalized_code

union all

SELECT * FROM tuva_synthetic.data_quality.procedure_modifier_1

union all

SELECT * FROM tuva_synthetic.data_quality.procedure_modifier_3

union all

SELECT * FROM tuva_synthetic.data_quality.procedure_modifier_2

union all

SELECT * FROM tuva_synthetic.data_quality.lab_result_source_abnormal_flag

union all

SELECT * FROM tuva_synthetic.data_quality.lab_result_specimen

union all

SELECT * FROM tuva_synthetic.data_quality.lab_result_source_reference_range_low

union all

SELECT * FROM tuva_synthetic.data_quality.lab_result_source_units

union all

SELECT * FROM tuva_synthetic.data_quality.lab_result_lab_result_id

union all

SELECT * FROM tuva_synthetic.data_quality.lab_result_collection_date

union all

SELECT * FROM tuva_synthetic.data_quality.lab_result_normalized_component

union all

SELECT * FROM tuva_synthetic.data_quality.lab_result_ordering_practitioner_id

union all

SELECT * FROM tuva_synthetic.data_quality.lab_result_result

union all

SELECT * FROM tuva_synthetic.data_quality.lab_result_source_code_type

union all

SELECT * FROM tuva_synthetic.data_quality.lab_result_normalized_description

union all

SELECT * FROM tuva_synthetic.data_quality.lab_result_normalized_reference_range_low

union all

SELECT * FROM tuva_synthetic.data_quality.lab_result_normalized_reference_range_high

union all

SELECT * FROM tuva_synthetic.data_quality.lab_result_normalized_code

union all

SELECT * FROM tuva_synthetic.data_quality.lab_result_source_description

union all

SELECT * FROM tuva_synthetic.data_quality.lab_result_status

union all

SELECT * FROM tuva_synthetic.data_quality.lab_result_accession_number

union all

SELECT * FROM tuva_synthetic.data_quality.lab_result_result_date

union all

SELECT * FROM tuva_synthetic.data_quality.lab_result_normalized_abnormal_flag

union all

SELECT * FROM tuva_synthetic.data_quality.lab_result_data_source

union all

SELECT * FROM tuva_synthetic.data_quality.lab_result_normalized_units

union all

SELECT * FROM tuva_synthetic.data_quality.lab_result_normalized_code_type

union all

SELECT * FROM tuva_synthetic.data_quality.lab_result_source_reference_range_high

union all

SELECT * FROM tuva_synthetic.data_quality.lab_result_source_code

union all

SELECT * FROM tuva_synthetic.data_quality.lab_result_patient_id

union all

SELECT * FROM tuva_synthetic.data_quality.lab_result_source_component

union all

SELECT * FROM tuva_synthetic.data_quality.lab_result_encounter_id

union all

SELECT * FROM tuva_synthetic.data_quality.patient_sex

union all

SELECT * FROM tuva_synthetic.data_quality.patient_state

union all

SELECT * FROM tuva_synthetic.data_quality.patient_city

union all

SELECT * FROM tuva_synthetic.data_quality.patient_longitude

union all

SELECT * FROM tuva_synthetic.data_quality.patient_county

union all

SELECT * FROM tuva_synthetic.data_quality.patient_race

union all

SELECT * FROM tuva_synthetic.data_quality.patient_death_flag

union all

SELECT * FROM tuva_synthetic.data_quality.patient_address

union all

SELECT * FROM tuva_synthetic.data_quality.patient_data_source

union all

SELECT * FROM tuva_synthetic.data_quality.patient_zip_code

union all

SELECT * FROM tuva_synthetic.data_quality.patient_first_name

union all

SELECT * FROM tuva_synthetic.data_quality.patient_last_name

union all

SELECT * FROM tuva_synthetic.data_quality.patient_latitude

union all

SELECT * FROM tuva_synthetic.data_quality.patient_birth_date

union all

SELECT * FROM tuva_synthetic.data_quality.patient_death_date

union all

SELECT * FROM tuva_synthetic.data_quality.patient_patient_id

union all

SELECT * FROM tuva_synthetic.data_quality.medication_rxnorm_code

union all

SELECT * FROM tuva_synthetic.data_quality.medication_source_code

union all

SELECT * FROM tuva_synthetic.data_quality.medication_atc_code

union all

SELECT * FROM tuva_synthetic.data_quality.medication_dispensing_date

union all

SELECT * FROM tuva_synthetic.data_quality.medication_prescribing_date

union all

SELECT * FROM tuva_synthetic.data_quality.medication_days_supply

union all

SELECT * FROM tuva_synthetic.data_quality.medication_strength

union all

SELECT * FROM tuva_synthetic.data_quality.medication_patient_id

union all

SELECT * FROM tuva_synthetic.data_quality.medication_rxnorm_description

union all

SELECT * FROM tuva_synthetic.data_quality.medication_encounter_id

union all

SELECT * FROM tuva_synthetic.data_quality.medication_data_source

union all

SELECT * FROM tuva_synthetic.data_quality.medication_atc_description

union all

SELECT * FROM tuva_synthetic.data_quality.medication_quantity_unit

union all

SELECT * FROM tuva_synthetic.data_quality.medication_source_description

union all

SELECT * FROM tuva_synthetic.data_quality.medication_ndc_code

union all

SELECT * FROM tuva_synthetic.data_quality.medication_medication_id

union all

SELECT * FROM tuva_synthetic.data_quality.medication_source_code_type

union all

SELECT * FROM tuva_synthetic.data_quality.medication_ndc_description

union all

SELECT * FROM tuva_synthetic.data_quality.medication_quantity

union all

SELECT * FROM tuva_synthetic.data_quality.medication_practitioner_id

union all

SELECT * FROM tuva_synthetic.data_quality.medication_route

union all

SELECT * FROM tuva_synthetic.data_quality.observation_source_code

union all

SELECT * FROM tuva_synthetic.data_quality.observation_normalized_reference_range_high

union all

SELECT * FROM tuva_synthetic.data_quality.observation_source_units

union all

SELECT * FROM tuva_synthetic.data_quality.observation_observation_type

union all

SELECT * FROM tuva_synthetic.data_quality.observation_normalized_code

union all

SELECT * FROM tuva_synthetic.data_quality.observation_normalized_description

union all

SELECT * FROM tuva_synthetic.data_quality.observation_data_source

union all

SELECT * FROM tuva_synthetic.data_quality.observation_panel_id

union all

SELECT * FROM tuva_synthetic.data_quality.observation_observation_id

union all

SELECT * FROM tuva_synthetic.data_quality.observation_source_reference_range_low

union all

SELECT * FROM tuva_synthetic.data_quality.observation_result

union all

SELECT * FROM tuva_synthetic.data_quality.observation_source_code_type

union all

SELECT * FROM tuva_synthetic.data_quality.observation_normalized_reference_range_low

union all

SELECT * FROM tuva_synthetic.data_quality.observation_observation_date

union all

SELECT * FROM tuva_synthetic.data_quality.observation_encounter_id

union all

SELECT * FROM tuva_synthetic.data_quality.observation_source_description

union all

SELECT * FROM tuva_synthetic.data_quality.observation_source_reference_range_high

union all

SELECT * FROM tuva_synthetic.data_quality.observation_normalized_units

union all

SELECT * FROM tuva_synthetic.data_quality.observation_normalized_code_type

union all

SELECT * FROM tuva_synthetic.data_quality.observation_patient_id

)

SELECT
    data_source,
    cast(source_date as TEXT) as source_date,
    table_name,
    drill_down_key,
    drill_down_value,
    field_name,
    bucket_name,
    invalid_reason,
    field_value,
    tuva_last_run,
    dense_rank() over (order by data_source, table_name, field_name) + 100000 as summary_sk
FROM CTE