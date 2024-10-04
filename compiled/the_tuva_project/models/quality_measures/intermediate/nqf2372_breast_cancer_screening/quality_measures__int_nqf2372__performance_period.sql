/*
    set performance period end to the end of the current calendar year
    or use the quality_measures_period_end variable if provided
*/
with period_end as (

    select
        cast('2018-12-31' as date)
         as performance_period_end
)

/*
    set performance period begin to a year and a day prior
    for a complete calendar year
*/
, period_begin as (

    select
          performance_period_end
        , 

    dateadd(
        day,
        1,
        

    dateadd(
        year,
        -1,
        performance_period_end
        )


        )

 as performance_period_begin
    from period_end

)

/*
    set performance lookback period to 27 months prior to the end of the
    performance period
*/
, period_lookback as (

    select
          performance_period_end
        , performance_period_begin
        , 

    dateadd(
        month,
        -27,
        performance_period_end
        )

 as performance_period_lookback
    from period_begin


)

select
      cast((select id
from tuva_synthetic.quality_measures._value_set_measures
where id = 'NQF2372') as TEXT) as measure_id
    , cast((select name
from tuva_synthetic.quality_measures._value_set_measures
where id = 'NQF2372') as TEXT) as measure_name
    , cast((select version
from tuva_synthetic.quality_measures._value_set_measures
where id = 'NQF2372') as TEXT) as measure_version
    , cast(performance_period_end as date) as performance_period_end
    , cast(performance_period_begin as date) as performance_period_begin
    , cast(performance_period_lookback as date) as performance_period_lookback
from period_lookback