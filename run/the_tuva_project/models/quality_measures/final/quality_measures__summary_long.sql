
  
    

        create or replace transient table tuva_synthetic.quality_measures.summary_long
         as
        (

/* measures should already be at the full eligibility population grain */
with  __dbt__cte__quality_measures__stg_core__patient as (

select
      patient_id
    , sex
    , birth_date
    , death_date
    , '2024-10-04 19:08:24.316902+00:00' as tuva_last_run
from tuva_synthetic.core.patient
), union_measures as (

    
    

        (
            select
                cast('tuva_synthetic.quality_measures._int_nqf2372_long' as TEXT) as _dbt_source_relation,

                
                    cast("PATIENT_ID" as character varying(16777216)) as "PATIENT_ID" ,
                    cast("DENOMINATOR_FLAG" as NUMBER(38,0)) as "DENOMINATOR_FLAG" ,
                    cast("NUMERATOR_FLAG" as NUMBER(38,0)) as "NUMERATOR_FLAG" ,
                    cast("EXCLUSION_FLAG" as NUMBER(38,0)) as "EXCLUSION_FLAG" ,
                    cast("EVIDENCE_DATE" as DATE) as "EVIDENCE_DATE" ,
                    cast("EVIDENCE_VALUE" as character varying(16777216)) as "EVIDENCE_VALUE" ,
                    cast("EXCLUSION_DATE" as DATE) as "EXCLUSION_DATE" ,
                    cast("EXCLUSION_REASON" as character varying(16777216)) as "EXCLUSION_REASON" ,
                    cast("PERFORMANCE_PERIOD_BEGIN" as DATE) as "PERFORMANCE_PERIOD_BEGIN" ,
                    cast("PERFORMANCE_PERIOD_END" as DATE) as "PERFORMANCE_PERIOD_END" ,
                    cast("MEASURE_ID" as character varying(16777216)) as "MEASURE_ID" ,
                    cast("MEASURE_NAME" as character varying(16777216)) as "MEASURE_NAME" ,
                    cast("MEASURE_VERSION" as character varying(16777216)) as "MEASURE_VERSION" ,
                    cast("TUVA_LAST_RUN" as character varying(32)) as "TUVA_LAST_RUN" 

            from tuva_synthetic.quality_measures._int_nqf2372_long

            
        )

        union all
        

        (
            select
                cast('tuva_synthetic.quality_measures._int_nqf0034_long' as TEXT) as _dbt_source_relation,

                
                    cast("PATIENT_ID" as character varying(16777216)) as "PATIENT_ID" ,
                    cast("DENOMINATOR_FLAG" as NUMBER(38,0)) as "DENOMINATOR_FLAG" ,
                    cast("NUMERATOR_FLAG" as NUMBER(38,0)) as "NUMERATOR_FLAG" ,
                    cast("EXCLUSION_FLAG" as NUMBER(38,0)) as "EXCLUSION_FLAG" ,
                    cast("EVIDENCE_DATE" as DATE) as "EVIDENCE_DATE" ,
                    cast("EVIDENCE_VALUE" as character varying(16777216)) as "EVIDENCE_VALUE" ,
                    cast("EXCLUSION_DATE" as DATE) as "EXCLUSION_DATE" ,
                    cast("EXCLUSION_REASON" as character varying(16777216)) as "EXCLUSION_REASON" ,
                    cast("PERFORMANCE_PERIOD_BEGIN" as DATE) as "PERFORMANCE_PERIOD_BEGIN" ,
                    cast("PERFORMANCE_PERIOD_END" as DATE) as "PERFORMANCE_PERIOD_END" ,
                    cast("MEASURE_ID" as character varying(16777216)) as "MEASURE_ID" ,
                    cast("MEASURE_NAME" as character varying(16777216)) as "MEASURE_NAME" ,
                    cast("MEASURE_VERSION" as character varying(16777216)) as "MEASURE_VERSION" ,
                    cast("TUVA_LAST_RUN" as character varying(32)) as "TUVA_LAST_RUN" 

            from tuva_synthetic.quality_measures._int_nqf0034_long

            
        )

        union all
        

        (
            select
                cast('tuva_synthetic.quality_measures._int_nqf0059_long' as TEXT) as _dbt_source_relation,

                
                    cast("PATIENT_ID" as character varying(16777216)) as "PATIENT_ID" ,
                    cast("DENOMINATOR_FLAG" as NUMBER(38,0)) as "DENOMINATOR_FLAG" ,
                    cast("NUMERATOR_FLAG" as NUMBER(38,0)) as "NUMERATOR_FLAG" ,
                    cast("EXCLUSION_FLAG" as NUMBER(38,0)) as "EXCLUSION_FLAG" ,
                    cast("EVIDENCE_DATE" as DATE) as "EVIDENCE_DATE" ,
                    cast("EVIDENCE_VALUE" as character varying(16777216)) as "EVIDENCE_VALUE" ,
                    cast("EXCLUSION_DATE" as DATE) as "EXCLUSION_DATE" ,
                    cast("EXCLUSION_REASON" as character varying(16777216)) as "EXCLUSION_REASON" ,
                    cast("PERFORMANCE_PERIOD_BEGIN" as DATE) as "PERFORMANCE_PERIOD_BEGIN" ,
                    cast("PERFORMANCE_PERIOD_END" as DATE) as "PERFORMANCE_PERIOD_END" ,
                    cast("MEASURE_ID" as character varying(16777216)) as "MEASURE_ID" ,
                    cast("MEASURE_NAME" as character varying(16777216)) as "MEASURE_NAME" ,
                    cast("MEASURE_VERSION" as character varying(16777216)) as "MEASURE_VERSION" ,
                    cast("TUVA_LAST_RUN" as character varying(32)) as "TUVA_LAST_RUN" 

            from tuva_synthetic.quality_measures._int_nqf0059_long

            
        )

        union all
        

        (
            select
                cast('tuva_synthetic.quality_measures._int_cqm236_long' as TEXT) as _dbt_source_relation,

                
                    cast("PATIENT_ID" as character varying(16777216)) as "PATIENT_ID" ,
                    cast("DENOMINATOR_FLAG" as NUMBER(38,0)) as "DENOMINATOR_FLAG" ,
                    cast("NUMERATOR_FLAG" as NUMBER(38,0)) as "NUMERATOR_FLAG" ,
                    cast("EXCLUSION_FLAG" as NUMBER(38,0)) as "EXCLUSION_FLAG" ,
                    cast("EVIDENCE_DATE" as DATE) as "EVIDENCE_DATE" ,
                    cast("EVIDENCE_VALUE" as character varying(16777216)) as "EVIDENCE_VALUE" ,
                    cast("EXCLUSION_DATE" as DATE) as "EXCLUSION_DATE" ,
                    cast("EXCLUSION_REASON" as character varying(16777216)) as "EXCLUSION_REASON" ,
                    cast("PERFORMANCE_PERIOD_BEGIN" as DATE) as "PERFORMANCE_PERIOD_BEGIN" ,
                    cast("PERFORMANCE_PERIOD_END" as DATE) as "PERFORMANCE_PERIOD_END" ,
                    cast("MEASURE_ID" as character varying(16777216)) as "MEASURE_ID" ,
                    cast("MEASURE_NAME" as character varying(16777216)) as "MEASURE_NAME" ,
                    cast("MEASURE_VERSION" as character varying(16777216)) as "MEASURE_VERSION" ,
                    cast("TUVA_LAST_RUN" as character varying(32)) as "TUVA_LAST_RUN" 

            from tuva_synthetic.quality_measures._int_cqm236_long

            
        )

        union all
        

        (
            select
                cast('tuva_synthetic.quality_measures._int_nqf0053_long' as TEXT) as _dbt_source_relation,

                
                    cast("PATIENT_ID" as character varying(16777216)) as "PATIENT_ID" ,
                    cast("DENOMINATOR_FLAG" as NUMBER(38,0)) as "DENOMINATOR_FLAG" ,
                    cast("NUMERATOR_FLAG" as NUMBER(38,0)) as "NUMERATOR_FLAG" ,
                    cast("EXCLUSION_FLAG" as NUMBER(38,0)) as "EXCLUSION_FLAG" ,
                    cast("EVIDENCE_DATE" as DATE) as "EVIDENCE_DATE" ,
                    cast("EVIDENCE_VALUE" as character varying(16777216)) as "EVIDENCE_VALUE" ,
                    cast("EXCLUSION_DATE" as DATE) as "EXCLUSION_DATE" ,
                    cast("EXCLUSION_REASON" as character varying(16777216)) as "EXCLUSION_REASON" ,
                    cast("PERFORMANCE_PERIOD_BEGIN" as DATE) as "PERFORMANCE_PERIOD_BEGIN" ,
                    cast("PERFORMANCE_PERIOD_END" as DATE) as "PERFORMANCE_PERIOD_END" ,
                    cast("MEASURE_ID" as character varying(16777216)) as "MEASURE_ID" ,
                    cast("MEASURE_NAME" as character varying(16777216)) as "MEASURE_NAME" ,
                    cast("MEASURE_VERSION" as character varying(16777216)) as "MEASURE_VERSION" ,
                    cast("TUVA_LAST_RUN" as character varying(32)) as "TUVA_LAST_RUN" 

            from tuva_synthetic.quality_measures._int_nqf0053_long

            
        )

        union all
        

        (
            select
                cast('tuva_synthetic.quality_measures._int_cbe0055_long' as TEXT) as _dbt_source_relation,

                
                    cast("PATIENT_ID" as character varying(16777216)) as "PATIENT_ID" ,
                    cast("DENOMINATOR_FLAG" as NUMBER(38,0)) as "DENOMINATOR_FLAG" ,
                    cast("NUMERATOR_FLAG" as NUMBER(38,0)) as "NUMERATOR_FLAG" ,
                    cast("EXCLUSION_FLAG" as NUMBER(38,0)) as "EXCLUSION_FLAG" ,
                    cast("EVIDENCE_DATE" as DATE) as "EVIDENCE_DATE" ,
                    cast("EVIDENCE_VALUE" as character varying(16777216)) as "EVIDENCE_VALUE" ,
                    cast("EXCLUSION_DATE" as DATE) as "EXCLUSION_DATE" ,
                    cast("EXCLUSION_REASON" as character varying(16777216)) as "EXCLUSION_REASON" ,
                    cast("PERFORMANCE_PERIOD_BEGIN" as DATE) as "PERFORMANCE_PERIOD_BEGIN" ,
                    cast("PERFORMANCE_PERIOD_END" as DATE) as "PERFORMANCE_PERIOD_END" ,
                    cast("MEASURE_ID" as character varying(16777216)) as "MEASURE_ID" ,
                    cast("MEASURE_NAME" as character varying(16777216)) as "MEASURE_NAME" ,
                    cast("MEASURE_VERSION" as character varying(16777216)) as "MEASURE_VERSION" ,
                    cast("TUVA_LAST_RUN" as character varying(32)) as "TUVA_LAST_RUN" 

            from tuva_synthetic.quality_measures._int_cbe0055_long

            
        )

        union all
        

        (
            select
                cast('tuva_synthetic.quality_measures._int_nqf0097_long' as TEXT) as _dbt_source_relation,

                
                    cast("PATIENT_ID" as character varying(16777216)) as "PATIENT_ID" ,
                    cast("DENOMINATOR_FLAG" as NUMBER(38,0)) as "DENOMINATOR_FLAG" ,
                    cast("NUMERATOR_FLAG" as NUMBER(38,0)) as "NUMERATOR_FLAG" ,
                    cast("EXCLUSION_FLAG" as NUMBER(38,0)) as "EXCLUSION_FLAG" ,
                    cast("EVIDENCE_DATE" as DATE) as "EVIDENCE_DATE" ,
                    cast("EVIDENCE_VALUE" as character varying(16777216)) as "EVIDENCE_VALUE" ,
                    cast("EXCLUSION_DATE" as DATE) as "EXCLUSION_DATE" ,
                    cast("EXCLUSION_REASON" as character varying(16777216)) as "EXCLUSION_REASON" ,
                    cast("PERFORMANCE_PERIOD_BEGIN" as DATE) as "PERFORMANCE_PERIOD_BEGIN" ,
                    cast("PERFORMANCE_PERIOD_END" as DATE) as "PERFORMANCE_PERIOD_END" ,
                    cast("MEASURE_ID" as character varying(16777216)) as "MEASURE_ID" ,
                    cast("MEASURE_NAME" as character varying(16777216)) as "MEASURE_NAME" ,
                    cast("MEASURE_VERSION" as character varying(16777216)) as "MEASURE_VERSION" ,
                    cast("TUVA_LAST_RUN" as character varying(32)) as "TUVA_LAST_RUN" 

            from tuva_synthetic.quality_measures._int_nqf0097_long

            
        )

        union all
        

        (
            select
                cast('tuva_synthetic.quality_measures._int_cqm438_long' as TEXT) as _dbt_source_relation,

                
                    cast("PATIENT_ID" as character varying(16777216)) as "PATIENT_ID" ,
                    cast("DENOMINATOR_FLAG" as NUMBER(38,0)) as "DENOMINATOR_FLAG" ,
                    cast("NUMERATOR_FLAG" as NUMBER(38,0)) as "NUMERATOR_FLAG" ,
                    cast("EXCLUSION_FLAG" as NUMBER(38,0)) as "EXCLUSION_FLAG" ,
                    cast("EVIDENCE_DATE" as DATE) as "EVIDENCE_DATE" ,
                    cast("EVIDENCE_VALUE" as character varying(16777216)) as "EVIDENCE_VALUE" ,
                    cast("EXCLUSION_DATE" as DATE) as "EXCLUSION_DATE" ,
                    cast("EXCLUSION_REASON" as character varying(16777216)) as "EXCLUSION_REASON" ,
                    cast("PERFORMANCE_PERIOD_BEGIN" as DATE) as "PERFORMANCE_PERIOD_BEGIN" ,
                    cast("PERFORMANCE_PERIOD_END" as DATE) as "PERFORMANCE_PERIOD_END" ,
                    cast("MEASURE_ID" as character varying(16777216)) as "MEASURE_ID" ,
                    cast("MEASURE_NAME" as character varying(16777216)) as "MEASURE_NAME" ,
                    cast("MEASURE_VERSION" as character varying(16777216)) as "MEASURE_VERSION" ,
                    cast("TUVA_LAST_RUN" as character varying(32)) as "TUVA_LAST_RUN" 

            from tuva_synthetic.quality_measures._int_cqm438_long

            
        )

        union all
        

        (
            select
                cast('tuva_synthetic.quality_measures._int_nqf0041_long' as TEXT) as _dbt_source_relation,

                
                    cast("PATIENT_ID" as character varying(16777216)) as "PATIENT_ID" ,
                    cast("DENOMINATOR_FLAG" as NUMBER(38,0)) as "DENOMINATOR_FLAG" ,
                    cast("NUMERATOR_FLAG" as NUMBER(38,0)) as "NUMERATOR_FLAG" ,
                    cast("EXCLUSION_FLAG" as NUMBER(38,0)) as "EXCLUSION_FLAG" ,
                    cast("EVIDENCE_DATE" as DATE) as "EVIDENCE_DATE" ,
                    cast("EVIDENCE_VALUE" as character varying(16777216)) as "EVIDENCE_VALUE" ,
                    cast("EXCLUSION_DATE" as DATE) as "EXCLUSION_DATE" ,
                    cast("EXCLUSION_REASON" as character varying(16777216)) as "EXCLUSION_REASON" ,
                    cast("PERFORMANCE_PERIOD_BEGIN" as DATE) as "PERFORMANCE_PERIOD_BEGIN" ,
                    cast("PERFORMANCE_PERIOD_END" as DATE) as "PERFORMANCE_PERIOD_END" ,
                    cast("MEASURE_ID" as character varying(16777216)) as "MEASURE_ID" ,
                    cast("MEASURE_NAME" as character varying(16777216)) as "MEASURE_NAME" ,
                    cast("MEASURE_VERSION" as character varying(16777216)) as "MEASURE_VERSION" ,
                    cast("TUVA_LAST_RUN" as character varying(32)) as "TUVA_LAST_RUN" 

            from tuva_synthetic.quality_measures._int_nqf0041_long

            
        )

        union all
        

        (
            select
                cast('tuva_synthetic.quality_measures._int_cbe0101_long' as TEXT) as _dbt_source_relation,

                
                    cast("PATIENT_ID" as character varying(16777216)) as "PATIENT_ID" ,
                    cast("DENOMINATOR_FLAG" as NUMBER(38,0)) as "DENOMINATOR_FLAG" ,
                    cast("NUMERATOR_FLAG" as NUMBER(38,0)) as "NUMERATOR_FLAG" ,
                    cast("EXCLUSION_FLAG" as NUMBER(38,0)) as "EXCLUSION_FLAG" ,
                    cast("EVIDENCE_DATE" as DATE) as "EVIDENCE_DATE" ,
                    cast("EVIDENCE_VALUE" as character varying(16777216)) as "EVIDENCE_VALUE" ,
                    cast("EXCLUSION_DATE" as DATE) as "EXCLUSION_DATE" ,
                    cast("EXCLUSION_REASON" as character varying(16777216)) as "EXCLUSION_REASON" ,
                    cast("PERFORMANCE_PERIOD_BEGIN" as DATE) as "PERFORMANCE_PERIOD_BEGIN" ,
                    cast("PERFORMANCE_PERIOD_END" as DATE) as "PERFORMANCE_PERIOD_END" ,
                    cast("MEASURE_ID" as character varying(16777216)) as "MEASURE_ID" ,
                    cast("MEASURE_NAME" as character varying(16777216)) as "MEASURE_NAME" ,
                    cast("MEASURE_VERSION" as character varying(16777216)) as "MEASURE_VERSION" ,
                    cast("TUVA_LAST_RUN" as character varying(32)) as "TUVA_LAST_RUN" 

            from tuva_synthetic.quality_measures._int_cbe0101_long

            
        )

        union all
        

        (
            select
                cast('tuva_synthetic.quality_measures._int_cqm48_long' as TEXT) as _dbt_source_relation,

                
                    cast("PATIENT_ID" as character varying(16777216)) as "PATIENT_ID" ,
                    cast("DENOMINATOR_FLAG" as NUMBER(38,0)) as "DENOMINATOR_FLAG" ,
                    cast("NUMERATOR_FLAG" as NUMBER(38,0)) as "NUMERATOR_FLAG" ,
                    cast("EXCLUSION_FLAG" as NUMBER(38,0)) as "EXCLUSION_FLAG" ,
                    cast("EVIDENCE_DATE" as DATE) as "EVIDENCE_DATE" ,
                    cast("EVIDENCE_VALUE" as character varying(16777216)) as "EVIDENCE_VALUE" ,
                    cast("EXCLUSION_DATE" as DATE) as "EXCLUSION_DATE" ,
                    cast("EXCLUSION_REASON" as character varying(16777216)) as "EXCLUSION_REASON" ,
                    cast("PERFORMANCE_PERIOD_BEGIN" as DATE) as "PERFORMANCE_PERIOD_BEGIN" ,
                    cast("PERFORMANCE_PERIOD_END" as DATE) as "PERFORMANCE_PERIOD_END" ,
                    cast("MEASURE_ID" as character varying(16777216)) as "MEASURE_ID" ,
                    cast("MEASURE_NAME" as character varying(16777216)) as "MEASURE_NAME" ,
                    cast("MEASURE_VERSION" as character varying(16777216)) as "MEASURE_VERSION" ,
                    cast("TUVA_LAST_RUN" as character varying(32)) as "TUVA_LAST_RUN" 

            from tuva_synthetic.quality_measures._int_cqm48_long

            
        )

        union all
        

        (
            select
                cast('tuva_synthetic.quality_measures._int_cqm130_long' as TEXT) as _dbt_source_relation,

                
                    cast("PATIENT_ID" as character varying(16777216)) as "PATIENT_ID" ,
                    cast("DENOMINATOR_FLAG" as NUMBER(38,0)) as "DENOMINATOR_FLAG" ,
                    cast("NUMERATOR_FLAG" as NUMBER(38,0)) as "NUMERATOR_FLAG" ,
                    cast("EXCLUSION_FLAG" as NUMBER(38,0)) as "EXCLUSION_FLAG" ,
                    cast("EVIDENCE_DATE" as DATE) as "EVIDENCE_DATE" ,
                    cast("EVIDENCE_VALUE" as character varying(16777216)) as "EVIDENCE_VALUE" ,
                    cast("EXCLUSION_DATE" as DATE) as "EXCLUSION_DATE" ,
                    cast("EXCLUSION_REASON" as character varying(16777216)) as "EXCLUSION_REASON" ,
                    cast("PERFORMANCE_PERIOD_BEGIN" as DATE) as "PERFORMANCE_PERIOD_BEGIN" ,
                    cast("PERFORMANCE_PERIOD_END" as DATE) as "PERFORMANCE_PERIOD_END" ,
                    cast("MEASURE_ID" as character varying(16777216)) as "MEASURE_ID" ,
                    cast("MEASURE_NAME" as character varying(16777216)) as "MEASURE_NAME" ,
                    cast("MEASURE_VERSION" as character varying(16777216)) as "MEASURE_VERSION" ,
                    cast("TUVA_LAST_RUN" as character varying(32)) as "TUVA_LAST_RUN" 

            from tuva_synthetic.quality_measures._int_cqm130_long

            
        )

        union all
        

        (
            select
                cast('tuva_synthetic.quality_measures._int_nqf0420_long' as TEXT) as _dbt_source_relation,

                
                    cast("PATIENT_ID" as character varying(16777216)) as "PATIENT_ID" ,
                    cast("DENOMINATOR_FLAG" as NUMBER(38,0)) as "DENOMINATOR_FLAG" ,
                    cast("NUMERATOR_FLAG" as NUMBER(38,0)) as "NUMERATOR_FLAG" ,
                    cast("EXCLUSION_FLAG" as NUMBER(38,0)) as "EXCLUSION_FLAG" ,
                    cast("EVIDENCE_DATE" as DATE) as "EVIDENCE_DATE" ,
                    cast("EVIDENCE_VALUE" as character varying(16777216)) as "EVIDENCE_VALUE" ,
                    cast("EXCLUSION_DATE" as DATE) as "EXCLUSION_DATE" ,
                    cast("EXCLUSION_REASON" as character varying(16777216)) as "EXCLUSION_REASON" ,
                    cast("PERFORMANCE_PERIOD_BEGIN" as DATE) as "PERFORMANCE_PERIOD_BEGIN" ,
                    cast("PERFORMANCE_PERIOD_END" as DATE) as "PERFORMANCE_PERIOD_END" ,
                    cast("MEASURE_ID" as character varying(16777216)) as "MEASURE_ID" ,
                    cast("MEASURE_NAME" as character varying(16777216)) as "MEASURE_NAME" ,
                    cast("MEASURE_VERSION" as character varying(16777216)) as "MEASURE_VERSION" ,
                    cast("TUVA_LAST_RUN" as character varying(32)) as "TUVA_LAST_RUN" 

            from tuva_synthetic.quality_measures._int_nqf0420_long

            
        )

        

)

, patient as (

    select distinct patient_id
    from __dbt__cte__quality_measures__stg_core__patient

)

/* selecting the full patient population as the grain of this table */
, joined as (

    select distinct
          patient.patient_id
        , union_measures.denominator_flag
        , union_measures.numerator_flag
        , union_measures.exclusion_flag
        , case
            when union_measures.exclusion_flag = 1 then null
            when union_measures.numerator_flag = 1 then 1
            when union_measures.denominator_flag = 1 then 0
            else null
          end as performance_flag
        , union_measures.evidence_date
        , union_measures.evidence_value
        , union_measures.exclusion_date
        , union_measures.exclusion_reason
        , union_measures.performance_period_begin
        , union_measures.performance_period_end
        , union_measures.measure_id
        , union_measures.measure_name
        , union_measures.measure_version
    from patient
        left join union_measures
            on patient.patient_id = union_measures.patient_id
)

, add_data_types as (

    select
          cast(patient_id as TEXT) as patient_id
        , cast(denominator_flag as integer) as denominator_flag
        , cast(numerator_flag as integer) as numerator_flag
        , cast(exclusion_flag as integer) as exclusion_flag
        , cast(performance_flag as integer) as performance_flag
        , cast(evidence_date as date) as evidence_date
        , cast(evidence_value as TEXT) as evidence_value
        , cast(exclusion_date as date) as exclusion_date
        , cast(exclusion_reason as TEXT) as exclusion_reason
        , cast(performance_period_begin as date) as performance_period_begin
        , cast(performance_period_end as date) as performance_period_end
        , cast(measure_id as TEXT) as measure_id
        , cast(measure_name as TEXT) as measure_name
        , cast(measure_version as TEXT) as measure_version
    from joined

)

select
      patient_id
    , denominator_flag
    , numerator_flag
    , exclusion_flag
    , performance_flag
    , evidence_date
    , evidence_value
    , exclusion_date
    , exclusion_reason
    , performance_period_begin
    , performance_period_end
    , measure_id
    , measure_name
    , measure_version
    , '2024-10-04 19:08:24.316902+00:00' as tuva_last_run
from add_data_types
        );
      
  