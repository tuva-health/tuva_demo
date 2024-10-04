with valid_hospice_exclusions as (

  select
      patient_id
    , exclusion_date
    , exclusion_reason
    , exclusion_type
  from tuva_synthetic.quality_measures._int_shared_exclusions_hospice_palliative
  where exclusion_date between (
  select 
    performance_period_begin
  from tuva_synthetic.quality_measures._int_nqf0097__performance_period

) and (
  select 
    performance_period_end
  from tuva_synthetic.quality_measures._int_nqf0097__performance_period

)
    and lower(exclusion_reason) in (
            'hospice encounter'
    )

)

, combined_exclusions as (

  select 
      valid_hospice_exclusions.patient_id
    , valid_hospice_exclusions.exclusion_date
    , valid_hospice_exclusions.exclusion_reason
    , valid_hospice_exclusions.exclusion_type
  from valid_hospice_exclusions
  inner join tuva_synthetic.quality_measures._int_nqf0097_denominator as denominator
      on valid_hospice_exclusions.patient_id = denominator.patient_id

)

, add_data_types as (

    select
        distinct
          cast(patient_id as TEXT) as patient_id
        , cast(exclusion_date as date) as exclusion_date
        , cast(exclusion_reason as TEXT) as exclusion_reason
        , 1 as exclusion_flag
    from combined_exclusions

)

select
      patient_id
    , exclusion_date
    , exclusion_reason
    , exclusion_flag
    , '2024-10-04 19:11:18.274664+00:00' as tuva_last_run
from add_data_types