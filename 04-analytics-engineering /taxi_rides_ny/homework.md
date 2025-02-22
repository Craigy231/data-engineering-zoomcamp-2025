## Module 4 Homework

For this homework, you will need the following datasets:
* [Green Taxi dataset (2019 and 2020)](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/green)
* [Yellow Taxi dataset (2019 and 2020)](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/yellow)
* [For Hire Vehicle dataset (2019)](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv)

### Before you start

1. Make sure you, **at least**, have them in GCS with a External Table **OR** a Native Table - use whichever method you prefer to accomplish that (Workflow Orchestration with [pandas-gbq](https://cloud.google.com/bigquery/docs/samples/bigquery-pandas-gbq-to-gbq-simple), [dlt for gcs](https://dlthub.com/docs/dlt-ecosystem/destinations/filesystem), [dlt for BigQuery](https://dlthub.com/docs/dlt-ecosystem/destinations/bigquery), [gsutil](https://cloud.google.com/storage/docs/gsutil), etc)
2. You should have exactly `7,778,101` records in your Green Taxi table
3. You should have exactly `109,047,518` records in your Yellow Taxi table
4. You should have exactly `43,244,696` records in your FHV table
5. Build the staging models for green/yellow as shown in [here](../../../04-analytics-engineering/taxi_rides_ny/models/staging/)
6. Build the dimension/fact for taxi_trips joining with `dim_zones`  as shown in [here](../../../04-analytics-engineering/taxi_rides_ny/models/core/fact_trips.sql)

**Note**: If you don't have access to GCP, you can spin up a local Postgres instance and ingest the datasets above


### Question 1: Understanding dbt model resolution

Provided you've got the following sources.yaml
```yaml
version: 2

sources:
  - name: raw_nyc_tripdata
    database: "{{ env_var('DBT_BIGQUERY_PROJECT', 'dtc_zoomcamp_2025') }}"
    schema:   "{{ env_var('DBT_BIGQUERY_SOURCE_DATASET', 'raw_nyc_tripdata') }}"
    tables:
      - name: ext_green_taxi
      - name: ext_yellow_taxi
```

with the following env variables setup where `dbt` runs:
```shell
export DBT_BIGQUERY_PROJECT=myproject
export DBT_BIGQUERY_DATASET=my_nyc_tripdata
```

What does this .sql model compile to?
```sql
select * 
from {{ source('raw_nyc_tripdata', 'ext_green_taxi' ) }}
```

- `select * from dtc_zoomcamp_2025.raw_nyc_tripdata.ext_green_taxi`
- `select * from dtc_zoomcamp_2025.my_nyc_tripdata.ext_green_taxi`
- `select * from myproject.raw_nyc_tripdata.ext_green_taxi`
- `select * from myproject.my_nyc_tripdata.ext_green_taxi`
- `select * from dtc_zoomcamp_2025.raw_nyc_tripdata.green_taxi`

### Solution: `select * from myproject.raw_nyc_tripdata.ext_green_taxi`


### Question 2: dbt Variables & Dynamic Models

Say you have to modify the following dbt_model (`fct_recent_taxi_trips.sql`) to enable Analytics Engineers to dynamically control the date range. 

- In development, you want to process only **the last 7 days of trips**
- In production, you need to process **the last 30 days** for analytics

```sql
select *
from {{ ref('fact_taxi_trips') }}
where pickup_datetime >= CURRENT_DATE - INTERVAL '30' DAY
```

What would you change to accomplish that in a such way that command line arguments takes precedence over ENV_VARs, which takes precedence over DEFAULT value?

- Add `ORDER BY pickup_datetime DESC` and `LIMIT {{ var("days_back", 30) }}`
- Update the WHERE clause to `pickup_datetime >= CURRENT_DATE - INTERVAL '{{ var("days_back", 30) }}' DAY`
- Update the WHERE clause to `pickup_datetime >= CURRENT_DATE - INTERVAL '{{ env_var("DAYS_BACK", "30") }}' DAY`
- Update the WHERE clause to `pickup_datetime >= CURRENT_DATE - INTERVAL '{{ var("days_back", env_var("DAYS_BACK", "30")) }}' DAY`
- Update the WHERE clause to `pickup_datetime >= CURRENT_DATE - INTERVAL '{{ env_var("DAYS_BACK", var("days_back", "30")) }}' DAY`

### Solution: Update the WHERE clause to `pickup_datetime >= CURRENT_DATE - INTERVAL '{{ var("days_back", env_var("DAYS_BACK", "30")) }}' DAY`

### Question 3: dbt Data Lineage and Execution

Considering the data lineage below **and** that taxi_zone_lookup is the **only** materialization build (from a .csv seed file):

![image](./homework_q2.png)

Select the option that does **NOT** apply for materializing `fct_taxi_monthly_zone_revenue`:

- `dbt run`
- `dbt run --select +models/core/dim_taxi_trips.sql+ --target prod`
- `dbt run --select +models/core/fct_taxi_monthly_zone_revenue.sql`
- `dbt run --select +models/core/`
- `dbt run --select models/staging/+`

### Solution: dbt run --select models/staging/+

### Question 4: dbt Macros and Jinja

Consider you're dealing with sensitive data (e.g.: [PII](https://en.wikipedia.org/wiki/Personal_data)), that is **only available to your team and very selected few individuals**, in the `raw layer` of your DWH (e.g: a specific BigQuery dataset or PostgreSQL schema), 

 - Among other things, you decide to obfuscate/masquerade that data through your staging models, and make it available in a different schema (a `staging layer`) for other Data/Analytics Engineers to explore

- And **optionally**, yet  another layer (`service layer`), where you'll build your dimension (`dim_`) and fact (`fct_`) tables (assuming the [Star Schema dimensional modeling](https://www.databricks.com/glossary/star-schema)) for Dashboarding and for Tech Product Owners/Managers

You decide to make a macro to wrap a logic around it:

```sql
{% macro resolve_schema_for(model_type) -%}

    {%- set target_env_var = 'DBT_BIGQUERY_TARGET_DATASET'  -%}
    {%- set stging_env_var = 'DBT_BIGQUERY_STAGING_DATASET' -%}

    {%- if model_type == 'core' -%} {{- env_var(target_env_var) -}}
    {%- else -%}                    {{- env_var(stging_env_var, env_var(target_env_var)) -}}
    {%- endif -%}

{%- endmacro %}
```

And use on your staging, dim_ and fact_ models as:
```sql
{{ config(
    schema=resolve_schema_for('core'), 
) }}
```

That all being said, regarding macro above, **select all statements that are true to the models using it**:
- Setting a value for  `DBT_BIGQUERY_TARGET_DATASET` env var is mandatory, or it'll fail to compile
- Setting a value for `DBT_BIGQUERY_STAGING_DATASET` env var is mandatory, or it'll fail to compile
- When using `core`, it materializes in the dataset defined in `DBT_BIGQUERY_TARGET_DATASET`
- When using `stg`, it materializes in the dataset defined in `DBT_BIGQUERY_STAGING_DATASET`, or defaults to `DBT_BIGQUERY_TARGET_DATASET`
- When using `staging`, it materializes in the dataset defined in `DBT_BIGQUERY_STAGING_DATASET`, or defaults to `DBT_BIGQUERY_TARGET_DATASET`

### Solution: All are true except the above second statement
- Setting a value for  `DBT_BIGQUERY_TARGET_DATASET` env var is mandatory, or it'll fail to compile
- When using `core`, it materializes in the dataset defined in `DBT_BIGQUERY_TARGET_DATASET`
- When using `stg`, it materializes in the dataset defined in `DBT_BIGQUERY_STAGING_DATASET`, or defaults to `DBT_BIGQUERY_TARGET_DATASET`
- When using `staging`, it materializes in the dataset defined in `DBT_BIGQUERY_STAGING_DATASET`, or defaults to `DBT_BIGQUERY_TARGET_DATASET`


## Serious SQL

Alright, in module 1, you had a SQL refresher, so now let's build on top of that with some serious SQL.

These are not meant to be easy - but they'll boost your SQL and Analytics skills to the next level.  
So, without any further do, let's get started...

You might want to add some new dimensions `year` (e.g.: 2019, 2020), `quarter` (1, 2, 3, 4), `year_quarter` (e.g.: `2019/Q1`, `2019-Q2`), and `month` (e.g.: 1, 2, ..., 12), **extracted from pickup_datetime**, to your `fct_taxi_trips` OR `dim_taxi_trips.sql` models to facilitate filtering your queries


### Question 5: Taxi Quarterly Revenue Growth

1. Create a new model `fct_taxi_trips_quarterly_revenue.sql`
2. Compute the Quarterly Revenues for each year for based on `total_amount`
3. Compute the Quarterly YoY (Year-over-Year) revenue growth 
  * e.g.: In 2020/Q1, Green Taxi had -12.34% revenue growth compared to 2019/Q1
  * e.g.: In 2020/Q4, Yellow Taxi had +34.56% revenue growth compared to 2019/Q4

Considering the YoY Growth in 2020, which were the yearly quarters with the best (or less worse) and worst results for green, and yellow

- green: {best: 2020/Q2, worst: 2020/Q1}, yellow: {best: 2020/Q2, worst: 2020/Q1}
- green: {best: 2020/Q2, worst: 2020/Q1}, yellow: {best: 2020/Q3, worst: 2020/Q4}
- green: {best: 2020/Q1, worst: 2020/Q2}, yellow: {best: 2020/Q2, worst: 2020/Q1}
- green: {best: 2020/Q1, worst: 2020/Q2}, yellow: {best: 2020/Q1, worst: 2020/Q2}
- green: {best: 2020/Q1, worst: 2020/Q2}, yellow: {best: 2020/Q3, worst: 2020/Q4}

### Solutions:
1. Create a new model `fct_taxi_trips_quarterly_revenue.sql`
```sql
{{ config(materialized='table') }}

with trips_data as (
    select *,
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(QUARTER FROM pickup_datetime) AS quarter,
        EXTRACT(MONTH FROM pickup_datetime) AS month,
        CONCAT(EXTRACT(YEAR FROM pickup_datetime), '/Q', EXTRACT(QUARTER FROM pickup_datetime)) AS year_quarter 
        
    from {{ ref('fact_trips') }}
)
    select 
    -- Revenue grouping 
    pickup_zone as revenue_zone,
    {{ dbt.date_trunc("month", "pickup_datetime") }} as revenue_month, 

    service_type, 

    -- Add new date components
    year,
    quarter,
    month,
    year_quarter,

    -- Revenue calculation 
    sum(fare_amount) as revenue_monthly_fare,
    sum(extra) as revenue_monthly_extra,
    sum(mta_tax) as revenue_monthly_mta_tax,
    sum(tip_amount) as revenue_monthly_tip_amount,
    sum(tolls_amount) as revenue_monthly_tolls_amount,
    sum(ehail_fee) as revenue_monthly_ehail_fee,
    sum(improvement_surcharge) as revenue_monthly_improvement_surcharge,
    sum(total_amount) as revenue_monthly_total_amount,

    -- Additional calculations
    count(tripid) as total_monthly_trips,
    avg(passenger_count) as avg_monthly_passenger_count,
    avg(trip_distance) as avg_monthly_trip_distance

    from trips_data
    group by 1,2,3,4,5,6,7 

```

Inside models/core directory, define the materialization type:
``` sql
{{
    config(
        materialized='table'
    )
}}
```
2. Compute the Quarterly Revenues for each year for based on `total_amount`
```sql
    SELECT
        year,
        quarter,
        year_quarter,
        service_type,
        SUM(revenue_monthly_total_amount)
    FROM {{ dm_monthly_zone_revenue }}
    GROUP BY 1,2,3,4
```
3. Compute the Quarterly YoY (Year-over-Year) revenue growth 
```sql
yoy_revenue AS (
    SELECT
        q.year,
        q.quarter,
        q.year_quarter,
        q.service_type,
        q.total_revenue,
        LAG(q.total_revenue) OVER (
            PARTITION BY q.service_type, qr.quarter
            ORDER BY qr.year
        ) AS prev_year_revenue,
        ROUND(
            (q.total_revenue - LAG(q.total_revenue) OVER (
                PARTITION BY q.service_type, q.quarter
                ORDER BY q.year
            )) / NULLIF(LAG(q.total_revenue) OVER (
                PARTITION BY q.service_type, q.quarter
                ORDER BY q.year
            ), 0) * 100, 2
        ) AS yoy_growth
    FROM quarterly_revenue qr
)

SELECT 
    year,
    quarter,
    year_quarter,
    service_type,
    total_revenue,         
    prev_year_revenue,     
    yoy_growth             
FROM yoy_revenue
```

Considering the YoY Growth in 2020, which were the yearly quarters with the best (or less worse) and worst results for green, and yellow:

### green: {best: 2020/Q1, worst: 2020/Q2}, yellow: {best: 2020/Q1, worst: 2020/Q2}

### Question 6: P97/P95/P90 Taxi Monthly Fare

### Solutions:
1. Create a new model `fct_taxi_trips_monthly_fare_p95.sql
2. Filter out invalid entries (`fare_amount > 0`, `trip_distance > 0`, and `payment_type_description in ('Cash', 'Credit Card')`)
```sql   
{{
    config(
        materialized = 'table'
    )
}}

WITH trip_fare_percentiles AS (
  SELECT 
    pickup_year
  , pickup_month
  , service_type
  , fare_amount
  , PERCENTILE_CONT(fare_amount, 0.5) OVER(PARTITION BY pickup_year, pickup_month, service_type) AS p50
  , PERCENTILE_CONT(fare_amount, 0.9) OVER(PARTITION BY pickup_year, pickup_month, service_type) AS p90
  , PERCENTILE_CONT(fare_amount, 0.95) OVER(PARTITION BY pickup_year, pickup_month, service_type) AS p95
  , PERCENTILE_CONT(fare_amount, 0.97) OVER(PARTITION BY pickup_year, pickup_month, service_type) AS p97
  FROM {{ ref('fact_trips') }}
  WHERE 1=1
        AND pickup_year >= 2019 
        AND pickup_year < 2021
        AND fare_amount > 0 
        AND trip_distance > 0
        AND LOWER(payment_type_description) IN ('cash', 'credit card')
)

SELECT 
  pickup_year
, pickup_month
, service_type
, MIN(fare_amount) AS min_fare_amount
, AVG(fare_amount) AS avg_fare_amount
, ANY_VALUE(p50) AS p50
, ANY_VALUE(p90) AS p90
, ANY_VALUE(p95) AS p95
, ANY_VALUE(p97) AS p97
, MAX(fare_amount) AS max_fare_amount
FROM trip_fare_percentiles
GROUP BY 1,2,3
```

2. Compute the **continous percentile** of `fare_amount` partitioning by service_type, year and and month
```sql   
SELECT * 
FROM `woven-catwalk-447100-j8.prod.fact_taxi_trips_monthly_fare_p95` 
WHERE 1=1
      AND pickup_year = 2020
      AND pickup_month = 4
ORDER BY service_type,1,2
```
Now, what are the values of `p97`, `p95`, `p90` for Green Taxi and Yellow Taxi, in April 2020?

- green: {p97: 55.0, p95: 45.0, p90: 26.5}, yellow: {p97: 52.0, p95: 37.0, p90: 25.5}
- green: {p97: 55.0, p95: 45.0, p90: 26.5}, yellow: {p97: 31.5, p95: 25.5, p90: 19.0}
- green: {p97: 40.0, p95: 33.0, p90: 24.5}, yellow: {p97: 52.0, p95: 37.0, p90: 25.5}
- green: {p97: 40.0, p95: 33.0, p90: 24.5}, yellow: {p97: 31.5, p95: 25.5, p90: 19.0}
- green: {p97: 55.0, p95: 45.0, p90: 26.5}, yellow: {p97: 52.0, p95: 25.5, p90: 19.0}

### green: {p97: 55.0, p95: 45.0, p90: 26.5}, yellow: {p97: 31.5, p95: 25.5, p90: 19.0}

### Question 7: Top #Nth longest P90 travel time Location for FHV

Prerequisites:
* Create a staging model for FHV Data (2019), and **DO NOT** add a deduplication step, just filter out the entries where `where dispatching_base_num is not null`
* Create a core model for FHV Data (`dim_fhv_trips.sql`) joining with `dim_zones`. Similar to what has been done [here](../../../04-analytics-engineering/taxi_rides_ny/models/core/fact_trips.sql)
* Add some new dimensions `year` (e.g.: 2019) and `month` (e.g.: 1, 2, ..., 12), based on `pickup_datetime`, to the core model to facilitate filtering for your queries

Now...
1. Create a new model `fct_fhv_monthly_zone_traveltime_p90.sql`
2. For each record in `dim_fhv_trips.sql`, compute the [timestamp_diff](https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#timestamp_diff) in seconds between dropoff_datetime and pickup_datetime - we'll call it `trip_duration` for this exercise
3. Compute the **continous** `p90` of `trip_duration` partitioning by year, month, pickup_location_id, and dropoff_location_id

For the Trips that **respectively** started from `Newark Airport`, `SoHo`, and `Yorkville East`, in November 2019, what are **dropoff_zones** with the 2nd longest p90 trip_duration ?

- LaGuardia Airport, Chinatown, Garment District
- LaGuardia Airport, Park Slope, Clinton East
- LaGuardia Airport, Saint Albans, Howard Beach
- LaGuardia Airport, Rosedale, Bath Beach
- LaGuardia Airport, Yorkville East, Greenpoint

### Solutions:
1. Create a new model `fct_fhv_monthly_zone_traveltime_p90.sql`
2. For each record in fact_fhv_trips.sql, compute the TIMESTAMP_DIFF() in seconds between dropoff_datetime and pickup_datetime, calling it trip_duration.
```sql   
{{
    config(
        materialized='table'
    )
}}

WITH fhv_trips AS (
  SELECT 
    pickup_year
  , pickup_month
  , pickup_locationid
  , pickup_zone
  , pickup_datetime
  , dropoff_locationid
  , dropoff_zone
  , dropoff_datetime
  , TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) AS trip_duration_sec
  FROM {{ ref('fact_fhv_trips') }}
)

, duration_percent AS (
  SELECT
    pickup_year
  , pickup_month
  , pickup_locationid
  , pickup_zone
  , pickup_datetime
  , dropoff_locationid
  , dropoff_zone
  , dropoff_datetime
  , trip_duration_sec
  , PERCENTILE_CONT(trip_duration_sec, 0.5) OVER(PARTITION BY pickup_year, pickup_month, pickup_locationid, dropoff_locationid) AS p50
  , PERCENTILE_CONT(trip_duration_sec, 0.9) OVER(PARTITION BY pickup_year, pickup_month, pickup_locationid, dropoff_locationid) AS p90
  , PERCENTILE_CONT(trip_duration_sec, 0.95) OVER(PARTITION BY pickup_year, pickup_month, pickup_locationid, dropoff_locationid) AS p95
  , PERCENTILE_CONT(trip_duration_sec, 0.97) OVER(PARTITION BY pickup_year, pickup_month, pickup_locationid, dropoff_locationid) AS p97 
  FROM fhv_trips
)

, final_data AS (
  SELECT 
    pickup_year
  , pickup_month
  , pickup_locationid
  , dropoff_locationid
  , pickup_zone
  , dropoff_zone 
  , COUNT(1) AS num_obs
  , MIN(trip_duration_sec) AS min_trip_duration_sec
  , AVG(trip_duration_sec) AS avg_trip_duration_sec
  , ANY_VALUE(p50) AS p50
  , ANY_VALUE(p90) AS p90
  , ANY_VALUE(p95) AS p95
  , ANY_VALUE(p97) AS p97
  , MAX(trip_duration_sec) AS max_trip_duration_sec
  FROM duration_percent
  GROUP BY 1,2,3,4,5,6
)

SELECT 
  pickup_year
, pickup_month
, pickup_zone
, dropoff_zone 
, num_obs
, min_trip_duration_sec
, avg_trip_duration_sec
, p50
, p90
, p95
, p97
, max_trip_duration_sec
FROM final_data 
WHERE 1=1
```

3. Compute the **continous** `p90` of `trip_duration` partitioning by year, month, pickup_location_id, and dropoff_location_id
```sql   
WITH continous_data AS (
  SELECT 
    pickup_year
  , pickup_month
  , pickup_zone
  , dropoff_zone 
  , num_obs 
  , num_distinct_obs
  , min_trip_duration_sec
  , avg_trip_duration_sec
  , p50
  , p90
  , p95
  , p97
  , max_trip_duration_sec
  , ROW_NUMBER() OVER(PARTITION BY pickup_Year, pickup_month, pickup_zone ORDER BY p90 DESC) AS p90_desc
  FROM continous_data
  WHERE 1=1
        AND pickup_zone IN ('Newark Airport', 'SoHo', 'Yorkville East')
        AND pickup_year = 2019
        AND pickup_month = 11
)

SELECT *
FROM final 
WHERE 1=1
      AND p90_desc = 2
```

### LaGuardia Airport, Chinatown, Garment District

## Submitting the solutions

* Form for submitting: https://courses.datatalks.club/de-zoomcamp-2025/homework/hw4


## Solution 

* To be published after deadline
