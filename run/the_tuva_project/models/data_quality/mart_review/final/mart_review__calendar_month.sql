
  
    

        create or replace transient table tuva_synthetic.data_quality.mart_review__calendar_month
         as
        (


select distinct
cast(year_month_int as TEXT) as year_month
, full_date
, '2024-10-04 19:08:24.316902+00:00' as tuva_last_run
from tuva_synthetic.reference_data.calendar c
where day = 1
        );
      
  