{{ config(
    materialized='table'
) }}

with trips as (

    select
        started_at,
        member_casual,
        rideable_type,
        ride_duration_minutes

    from {{ source('silver', 'trips') }}

),

hourly_demand as (

    select
        cast(started_at as date) as ride_date,
        year(started_at) as ride_year,
        month(started_at) as ride_month,
        date_format(started_at, 'EEEE') as day_of_week,
        hour(started_at) as ride_hour,

        member_casual,
        rideable_type,

        count(*) as total_trips,

        round(
            avg(ride_duration_minutes),
            2
        ) as avg_ride_duration_minutes

    from trips

    group by
        cast(started_at as date),
        year(started_at),
        month(started_at),
        date_format(started_at, 'EEEE'),
        hour(started_at),
        member_casual,
        rideable_type
)

select *
from hourly_demand