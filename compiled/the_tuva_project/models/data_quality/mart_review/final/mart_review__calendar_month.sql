


select distinct
cast(year_month_int as TEXT) as year_month
, full_date
, '2024-10-04 19:11:18.274664+00:00' as tuva_last_run
from tuva_synthetic.reference_data.calendar c
where day = 1