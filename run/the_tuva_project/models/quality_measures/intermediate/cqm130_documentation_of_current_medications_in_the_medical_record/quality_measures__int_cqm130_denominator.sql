
  
    

        create or replace transient table tuva_synthetic.quality_measures._int_cqm130_denominator
         as
        (

with  __dbt__cte__quality_measures__stg_core__encounter as (


select
      patient_id
    , encounter_id
    , encounter_type
    , length_of_stay
    , encounter_start_date
    , encounter_end_date
    , '2024-10-04 19:08:24.316902+00:00' as tuva_last_run
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
    , '2024-10-04 19:08:24.316902+00:00' as tuva_last_run
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
    , '2024-10-04 19:08:24.316902+00:00' as tuva_last_run
from tuva_synthetic.core.patient
), visit_codes as (

    select
          code
        , code_system
    from tuva_synthetic.quality_measures._value_set_codes
    where lower(concept_name) in (
            'encounter to document medications'
    )

)

, visits_encounters as (

    select 
          patient_id
        , coalesce(encounter.encounter_start_date,encounter.encounter_end_date) as procedure_encounter_date -- alias only to enable union later
        , coalesce(encounter.encounter_end_date,encounter.encounter_start_date) as claims_encounter_date -- alias only to enable union later
    from __dbt__cte__quality_measures__stg_core__encounter encounter
    inner join tuva_synthetic.quality_measures._int_cqm130__performance_period as pp
        on coalesce(encounter.encounter_end_date,encounter.encounter_start_date) >= pp.performance_period_begin
            and coalesce(encounter.encounter_start_date,encounter.encounter_end_date) <= pp.performance_period_end
    where lower(encounter_type) in (
          'home health'
        , 'office visit'
        , 'outpatient'
        , 'outpatient rehabilitation'
        , 'telehealth'
    )

)

, procedure_encounters as (

    select 
          patient_id
        , procedure_date as procedure_encounter_date
        , try_cast( null as date ) as claims_encounter_date
    from __dbt__cte__quality_measures__stg_core__procedure procs
    inner join tuva_synthetic.quality_measures._int_cqm130__performance_period as pp
        on procedure_date between pp.performance_period_begin and pp.performance_period_end
    inner join visit_codes
        on coalesce(procs.normalized_code,procs.source_code) = visit_codes.code

)

, claims_encounters as (
    
    select 
          patient_id
        , try_cast( null as date ) as procedure_encounter_date
        , coalesce(claim_end_date,claim_start_date) as claims_encounter_date
    from __dbt__cte__quality_measures__stg_medical_claim medical_claim
    inner join tuva_synthetic.quality_measures._int_cqm130__performance_period as pp 
        on coalesce(claim_end_date,claim_start_date)  >=  pp.performance_period_begin
            and coalesce(claim_start_date,claim_end_date) <=  pp.performance_period_end
    inner join visit_codes
        on medical_claim.hcpcs_code = visit_codes.code

)

, all_encounters as (

    select *, 'v' as visit_enc, cast(null as TEXT) as proc_enc, cast(null as TEXT) as claim_enc
    from visits_encounters

    union all

    select *, cast(null as TEXT) as visit_enc, 'p' as proc_enc, cast(null as TEXT) as claim_enc
    from procedure_encounters

    union all
    
    select *, cast(null as TEXT) as visit_enc, cast(null as TEXT) as proc_enc, 'c' as claim_enc
    from claims_encounters

)

, multiple_encounters_by_patient as (

    select
          patient_id
        , procedure_encounter_date
        , claims_encounter_date
        , case when procedure_encounter_date >= claims_encounter_date
                then procedure_encounter_date
            else claims_encounter_date
          end as max_encounter_date
        , coalesce(min(visit_enc), '') || coalesce(min(proc_enc), '') || coalesce(min(claim_enc), '') as qualifying_types
    from all_encounters
    group by patient_id, procedure_encounter_date, claims_encounter_date

)

, max_encounter_dates_by_patient as (

	select
		  patient_id
		, max(max_encounter_date) as max_encounter_date
	from multiple_encounters_by_patient
	group by patient_id

)

, latest_patient_encounters as (
	
	select
		  max_encounter_dates_by_patient.patient_id
		, max_encounter_dates_by_patient.max_encounter_date
		, procedure_encounter_date
		, claims_encounter_date
	from max_encounter_dates_by_patient
	inner join multiple_encounters_by_patient
		on max_encounter_dates_by_patient.patient_id = multiple_encounters_by_patient.patient_id

)

, patients_with_age as (

    select
          p.patient_id
        , procedure_encounter_date
        , claims_encounter_date
        , floor(datediff(
        hour,
        birth_date,
        e.max_encounter_date
        ) / 8760.0) as max_age
    from __dbt__cte__quality_measures__stg_core__patient p
    inner join latest_patient_encounters e
        on p.patient_id = e.patient_id
    where p.death_date is null

)

, qualifying_patients as (

    select
        distinct
          patients_with_age.patient_id
        , patients_with_age.max_age as age
        , patients_with_age.procedure_encounter_date
        , patients_with_age.claims_encounter_date
        , pp.performance_period_begin
        , pp.performance_period_end
        , pp.measure_id
        , pp.measure_name
        , pp.measure_version
        , 1 as denominator_flag
    from patients_with_age
    cross join tuva_synthetic.quality_measures._int_cqm130__performance_period pp
    where max_age >= 18
    
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
        , cast(procedure_encounter_date as date) as procedure_encounter_date
        , cast(claims_encounter_date as date) as claims_encounter_date
        , cast(denominator_flag as integer) as denominator_flag
    from qualifying_patients

)

select 
      patient_id
    , age
    , procedure_encounter_date
    , claims_encounter_date
    , performance_period_begin
    , performance_period_end
    , measure_id
    , measure_name
    , measure_version
    , denominator_flag
    , '2024-10-04 19:08:24.316902+00:00' as tuva_last_run
from add_data_types
        );
      
  