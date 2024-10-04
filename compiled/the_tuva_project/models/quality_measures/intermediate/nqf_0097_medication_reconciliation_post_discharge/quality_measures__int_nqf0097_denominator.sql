

with  __dbt__cte__quality_measures__stg_core__encounter as (


select
      patient_id
    , encounter_id
    , encounter_type
    , length_of_stay
    , encounter_start_date
    , encounter_end_date
    , '2024-10-04 19:11:18.274664+00:00' as tuva_last_run
from tuva_synthetic.core.encounter


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
),  __dbt__cte__quality_measures__stg_medical_claim as (



select
         cast(null as TEXT ) as patient_id
        , cast(null as TEXT ) as claim_id
        , try_cast( null as date ) as claim_start_date
        , try_cast( null as date ) as claim_end_date
        , cast(null as TEXT ) as place_of_service_code
        , cast(null as TEXT ) as hcpcs_code
        , cast(null as TEXT ) as hcpcs_modifier_1
        , cast(null as TEXT ) as hcpcs_modifier_2
        , cast(null as TEXT ) as hcpcs_modifier_3
        , cast(null as TEXT ) as hcpcs_modifier_4
        , cast(null as TEXT ) as hcpcs_modifier_5
        , cast(null as TIMESTAMP ) as tuva_last_run
    limit 0
),  __dbt__cte__quality_measures__stg_core__patient as (

select
      patient_id
    , sex
    , birth_date
    , death_date
    , '2024-10-04 19:11:18.274664+00:00' as tuva_last_run
from tuva_synthetic.core.patient
), visit_codes as (

    select
          concept_name
        , code
        , code_system
    from tuva_synthetic.quality_measures._value_set_codes

)

, valid_concepts as (

    select
        concept_name
    from tuva_synthetic.quality_measures._value_set_concepts
    where concept_name in (
          'annual wellness visit'
        , 'care services in long term residential facility'
        , 'encounter to document medications'
        , 'home healthcare services'
        , 'office visit'
        , 'outpatient'
        , 'psychoanalysis'
    )

)

, valid_visit_codes as (

    select
          visit_codes.concept_name
        , visit_codes.code
        , visit_codes.code_system
    from visit_codes
    inner join valid_concepts
        on visit_codes.concept_name = valid_concepts.concept_name

)

, visits_encounters as (

    select patient_id
         , length_of_stay
         , encounter.encounter_end_date
         , coalesce(encounter.encounter_start_date,encounter.encounter_end_date) as min_date
         , coalesce(encounter.encounter_end_date,encounter.encounter_start_date) as max_date
    from __dbt__cte__quality_measures__stg_core__encounter encounter
    inner join tuva_synthetic.quality_measures._int_nqf0097__performance_period as pp
        on coalesce(encounter.encounter_end_date,encounter.encounter_start_date) >= pp.performance_period_begin
            and coalesce(encounter.encounter_start_date,encounter.encounter_end_date) <= pp.performance_period_end
    -- all encounter types considered; inpatient encounters are filtered by length of stay being more than 0 days

)

, procedure_encounters as (

    select 
          patient_id
        , procedure_date as min_date
        , procedure_date as max_date
        from __dbt__cte__quality_measures__stg_core__procedure procs

    inner join tuva_synthetic.quality_measures._int_nqf0097__performance_period  as pp
        on procedure_date between pp.performance_period_begin and  pp.performance_period_end
    inner join valid_visit_codes
            on coalesce(procs.normalized_code,procs.source_code) = valid_visit_codes.code

)

, claims_encounters as (
    
    select patient_id
    , coalesce(claim_start_date,claim_end_date) as min_date
    , coalesce(claim_end_date,claim_start_date) as max_date
    from __dbt__cte__quality_measures__stg_medical_claim medical_claim
    inner join tuva_synthetic.quality_measures._int_nqf0097__performance_period  as pp on
        coalesce(claim_end_date,claim_start_date)  >=  pp.performance_period_begin
         and coalesce(claim_start_date,claim_end_date) <=  pp.performance_period_end
    inner join  valid_visit_codes
        on medical_claim.hcpcs_code= valid_visit_codes.code

)

, all_encounters as (

    select
          patient_id
        , min_date
        , max_date
        , 'v' as visit_enc
        , cast(null as TEXT) as proc_enc
        , cast(null as TEXT) as claim_enc
    from visits_encounters

    union all

    select
          patient_id
        , min_date
        , max_date
        , cast(null as TEXT) as visit_enc
        , 'p' as proc_enc
        , cast(null as TEXT) as claim_enc
    from procedure_encounters

    union all
    
    select
          patient_id
        , min_date
        , max_date
        , cast(null as TEXT) as visit_enc
        , cast(null as TEXT) as proc_enc
        , 'c' as claim_enc
    from claims_encounters

)

, encounters_by_patient as (

    select patient_id,min(min_date) min_date, max(max_date) max_date,
        concat(concat(
            coalesce(min(visit_enc),'')
            ,coalesce(min(proc_enc),''))
            ,coalesce(min(claim_enc),'')
            ) as qualifying_types
    from all_encounters
    group by patient_id

)

, patients_with_age as (

    select
          p.patient_id
        , min_date
        , floor(datediff(
        hour,
        birth_date,
        e.min_date
        ) / 8760.0)  as min_age
        , max_date
        , floor(datediff(
        hour,
        birth_date,
        e.max_date
        ) / 8760.0) as max_age
        , qualifying_types
    from __dbt__cte__quality_measures__stg_core__patient p
    inner join encounters_by_patient e
        on p.patient_id = e.patient_id
    where p.death_date is null

)

, qualifying_patients as (

    select
        distinct
          patients_with_age.patient_id
        , patients_with_age.max_age as age
        , pp.performance_period_begin
        , pp.performance_period_end
        , pp.measure_id
        , pp.measure_name
        , pp.measure_version
        , visits_encounters.encounter_end_date as discharge_date
        , 1 as denominator_flag
    from patients_with_age
    cross join tuva_synthetic.quality_measures._int_nqf0097__performance_period pp
    inner join visits_encounters
        on patients_with_age.patient_id = visits_encounters.patient_id
    where max_age >= 18
        and visits_encounters.length_of_stay > 0 --ensures inpatient
)

, add_data_types as (

    select
          cast(patient_id as TEXT) as patient_id
        , cast(age as integer) as age
        , cast(performance_period_begin as date) as performance_period_begin
        , cast(performance_period_end as date) as performance_period_end
        , cast(measure_id as TEXT) as measure_id
        , cast(measure_name as TEXT) as measure_name
        , cast(measure_version as TEXT) as measure_version
        , cast(discharge_date as date) as discharge_date
        , cast(denominator_flag as integer) as denominator_flag
    from qualifying_patients

)

select 
      patient_id
    , age
    , performance_period_begin
    , performance_period_end
    , measure_id
    , measure_name
    , measure_version
    , discharge_date
    , denominator_flag
    , '2024-10-04 19:11:18.274664+00:00' as tuva_last_run
from add_data_types