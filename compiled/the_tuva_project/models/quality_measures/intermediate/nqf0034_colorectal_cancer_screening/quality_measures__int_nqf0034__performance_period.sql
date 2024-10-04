
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
    set performance lookback periods for each type of test

    during the measurement period:
    - Fecal occult blood test (FOBT) during the measurement period

    during the measurement period or the two years prior:
    - Fecal immunochemical DNA test (FIT-DNA)

    during the measurement period or the four years prior:
    - Flexible sigmoidoscopy
    - Computed tomography (CT) colonography

    during the measurement period or the nine years prior:
    - Colonoscopy

*/
, period_lookback as (

    select
          performance_period_end
        , performance_period_begin
        , 

    dateadd(
        year,
        -2,
        performance_period_end
        )

 as performance_period_begin_1yp
        , 

    dateadd(
        year,
        -3,
        performance_period_end
        )

 as performance_period_begin_2yp
        , 

    dateadd(
        year,
        -5,
        performance_period_end
        )

 as performance_period_begin_4yp
        , 

    dateadd(
        year,
        -10,
        performance_period_end
        )

 as performance_period_begin_9yp
    from period_begin

)

select
      cast(performance_period_begin as date) as performance_period_begin
    , cast(performance_period_end as date) as performance_period_end
    , cast(performance_period_begin_1yp as date) as performance_period_begin_1yp
    , cast(performance_period_begin_2yp as date) as performance_period_begin_2yp
    , cast(performance_period_begin_4yp as date) as performance_period_begin_4yp
    , cast(performance_period_begin_9yp as date) as performance_period_begin_9yp
from period_lookback