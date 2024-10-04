

with

combined_exclusions as (

  select
      exclusions.*
    , denominator.age
  from tuva_synthetic.quality_measures._int_nqf0053_exclusions_stage_1 as exclusions
  inner join tuva_synthetic.quality_measures._int_nqf0053_denominator as denominator
      on exclusions.patient_id = denominator.patient_id

)

, valid_exclusions as (

    select
        *
    from combined_exclusions
    where exclusion_type = 'institutional_snp'
    and age >= 66

    union all

    select
        *
    from combined_exclusions
    where exclusion_type in
    (
        'advanced_illness'
      , 'dementia'
    )
    and age between 66 and 80

    union all

    select
      *
    from combined_exclusions
    where exclusion_type = 'measure specific exclusion for defined window'
    and age >= 81

    union all

    select
        *
    from combined_exclusions
    where exclusion_type in
    (
        'measure specific exclusion for procedure medication'
      , 'hospice_palliative'
    )

)


, add_data_types as (

    select
        distinct
          cast(patient_id as TEXT) as patient_id
        , cast(exclusion_date as date) as exclusion_date
        , cast(exclusion_reason as TEXT) as exclusion_reason
        , 1 as exclusion_flag
    from valid_exclusions

)

select
      patient_id
    , exclusion_date
    , exclusion_reason
    , exclusion_flag
    , '2024-10-04 19:11:18.274664+00:00' as tuva_last_run
from add_data_types