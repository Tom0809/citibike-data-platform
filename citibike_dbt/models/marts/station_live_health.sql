{{ config(
    materialized='table'
) }}

with stations as (

    select
        station_id,
        address,
        lat,
        lon,
        capacity,

        num_bikes_available,
        num_ebikes_available,
        num_bikes_disabled,

        num_docks_available,
        num_docks_disabled,

        is_installed,
        is_renting,
        is_returning,

        last_reported_at,
        status_last_updated_at,
        _silver_updated_at

    from {{ source('silver', 'stations') }}

),

health_metrics as (

    select
        *,

        case
            when capacity > 0 then
                round(
                    num_bikes_available * 100.0 / capacity,
                    2
                )
        end as bike_availability_pct,

        case
            when capacity > 0 then
                round(
                    num_docks_available * 100.0 / capacity,
                    2
                )
        end as dock_availability_pct,

        coalesce(num_bikes_disabled, 0)
            + coalesce(num_docks_disabled, 0)
            as disabled_assets

    from stations
),

station_health as (

    select
        *,

        case
            when is_installed = 0
              or (is_renting = 0 and is_returning = 0)
                then 'OUT_OF_SERVICE'

            when num_bikes_available = 0
                then 'NO_BIKES'

            when num_docks_available = 0
                then 'NO_DOCKS'

            when bike_availability_pct <= 10
                then 'LOW_BIKES'

            when dock_availability_pct <= 10
                then 'LOW_DOCKS'

            else 'HEALTHY'
        end as health_status

    from health_metrics
)

select
    station_id,
    address,
    lat,
    lon,
    capacity,

    num_bikes_available,
    num_ebikes_available,
    num_bikes_disabled,

    num_docks_available,
    num_docks_disabled,

    bike_availability_pct,
    dock_availability_pct,
    disabled_assets,

    is_installed,
    is_renting,
    is_returning,

    health_status,

    last_reported_at,
    status_last_updated_at,
    _silver_updated_at

from station_health