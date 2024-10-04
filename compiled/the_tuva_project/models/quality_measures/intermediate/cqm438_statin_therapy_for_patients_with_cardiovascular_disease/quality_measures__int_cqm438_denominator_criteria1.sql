

with  __dbt__cte__quality_measures__stg_core__condition as (

select
      patient_id
    , claim_id
    , encounter_id
    , recorded_date
    , source_code_type
    , source_code
    , normalized_code_type
    , normalized_code
    , '2024-10-04 19:11:18.274664+00:00' as tuva_last_run
from tuva_synthetic.core.condition
),  __dbt__cte__quality_measures__stg_core__procedure as (

select
      patient_id
    , encounter_id
    , procedure_date
    , source_code_type
    , source_code
    , normalized_code_type
    , normalized_code
    , modifier_1
    , modifier_2
    , modifier_3
    , modifier_4
    , modifier_5
    , '2024-10-04 19:11:18.274664+00:00' as tuva_last_run
from tuva_synthetic.core.procedure
), ascvd_codes as (

    select
          code
        , code_system
    from tuva_synthetic.quality_measures._value_set_codes
    where lower(concept_name) in (
              'atherosclerosis and peripheral arterial disease'
            , 'myocardial infarction'
            , 'pci'
            , 'stable and unstable angina'
            , 'cabg or pci procedure'
            , 'cabg surgeries'
            , 'cerebrovascular disease stroke or tia'
            , 'ischemic heart disease or related diagnoses'
            , 'carotid intervention'
        )

)

, conditions as (

    select
          patient_id
        , claim_id
        , encounter_id
        , recorded_date
        , source_code
        , source_code_type
        , normalized_code
        , normalized_code_type
    from __dbt__cte__quality_measures__stg_core__condition

)

, ascvd_conditions as (

    select
          conditions.patient_id
        , conditions.recorded_date as evidence_date
    from conditions
    inner join ascvd_codes
        on coalesce(conditions.normalized_code_type, conditions.source_code_type) = ascvd_codes.code_system
            and coalesce(conditions.normalized_code, conditions.source_code) = ascvd_codes.code

)

, procedures as (

    select
          patient_id
        , procedure_date
        , coalesce (
              normalized_code_type
            , case
                when lower(source_code_type) = 'cpt' then 'hcpcs'
                when lower(source_code_type) = 'snomed' then 'snomed-ct'
                else lower(source_code_type)
              end
          ) as code_type
        , coalesce(
              normalized_code
            , source_code
          ) as code
    from __dbt__cte__quality_measures__stg_core__procedure

)

, ascvd_procedures as (

    select
          procedures.patient_id
        , procedures.procedure_date as evidence_date
    from procedures
         inner join ascvd_codes
             on procedures.code = ascvd_codes.code
             and procedures.code_type = ascvd_codes.code_system

)

, historical_ascvd as (

    select
          ascvd_conditions.patient_id
        , ascvd_conditions.evidence_date
    from ascvd_conditions

    union all

    select
          ascvd_procedures.patient_id
        , ascvd_procedures.evidence_date
    from ascvd_procedures

)

, patients_with_ascvd as (

    select
        distinct
          historical_ascvd.patient_id
        , pp.performance_period_begin
        , pp.performance_period_end
        , pp.measure_id
        , pp.measure_name
        , pp.measure_version
    from historical_ascvd
    inner join tuva_synthetic.quality_measures._int_cqm438__performance_period pp
    on evidence_date <= pp.performance_period_end

)

, add_data_types as (

    select
          cast(patient_id as TEXT) as patient_id
        , cast(performance_period_begin as date) as performance_period_begin
        , cast(performance_period_end as date) as performance_period_end
        , cast(measure_id as TEXT) as measure_id
        , cast(measure_name as TEXT) as measure_name
        , cast(measure_version as TEXT) as measure_version
    from patients_with_ascvd

)

select 
      patient_id
    , performance_period_begin
    , performance_period_end
    , measure_id
    , measure_name
    , measure_version
    , '2024-10-04 19:11:18.274664+00:00' as tuva_last_run
from add_data_types