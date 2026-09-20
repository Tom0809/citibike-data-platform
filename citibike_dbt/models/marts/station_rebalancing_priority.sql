{{ config(
    materialized='table'
) }}

with station_health as (

    select
        station_id,
        address,
        lat,
        lon,
        capacity,

        num_bikes_available,
        num_docks_available,

        bike_availability_pct,
        dock_availability_pct,

        num_bikes_disabled,
        num_docks_disabled,

        is_renting,
        is_returning,

        health_status,

        last_reported_at,
        status_last_updated_at

    from {{ ref('station_live_health') }}

    where health_status != 'HEALTHY'
),

priority_logic as (

    select
        *,

        case
            when health_status = 'OUT_OF_SERVICE'
                then 'INVESTIGATE'

            when health_status in ('NO_BIKES', 'LOW_BIKES')
                then 'ADD_BIKES'

            when health_status in ('NO_DOCKS', 'LOW_DOCKS')
                then 'REMOVE_BIKES'
        end as recommended_action,

        case
            when health_status in ('NO_BIKES', 'NO_DOCKS')
                then 'CRITICAL'

            when health_status = 'OUT_OF_SERVICE'
                then 'HIGH'

            when health_status in ('LOW_BIKES', 'LOW_DOCKS')
                then 'MEDIUM'
        end as priority_level,

        case
            when health_status in ('NO_BIKES', 'NO_DOCKS')
                then 100.0

            when health_status = 'OUT_OF_SERVICE'
                then 95.0

            when health_status = 'LOW_BIKES'
                then round(
                    90.0 - bike_availability_pct,
                    2
                )

            when health_status = 'LOW_DOCKS'
                then round(
                    90.0 - dock_availability_pct,
                    2
                )
        end as priority_score

    from station_health
),

ranked_stations as (

    select
        *,

        row_number() over (
            order by
                priority_score desc,
                capacity desc,
                station_id
        ) as priority_rank

    from priority_logic
)

select
    priority_rank,

    station_id,
    address,
    lat,
    lon,
    capacity,

    num_bikes_available,
    num_docks_available,

    bike_availability_pct,
    dock_availability_pct,

    num_bikes_disabled,
    num_docks_disabled,

    health_status,
    priority_level,
    priority_score,
    recommended_action,

    is_renting,
    is_returning,

    last_reported_at,
    status_last_updated_at

from ranked_stations