{{ config(
    materialized='table'
) }}

with station_mapping as (

    select
        cast(station_id as string) as station_id,
        canonical_station_key,
        canonical_station_name

    from {{ ref('station_mapping') }}
),

departures as (

    select
        year(t.started_at) as ride_year,
        month(t.started_at) as ride_month,

        coalesce(
            m.canonical_station_key,
            t.start_station_id
        ) as station_key,

        max(
            coalesce(
                m.canonical_station_name,
                t.start_station_name
            )
        ) as station_name,

        round(avg(t.start_lat), 6) as station_lat,
        round(avg(t.start_lng), 6) as station_lng,

        count(*) as departures

    from {{ source('silver', 'trips') }} t

    left join station_mapping m
        on t.start_station_id = m.station_id

    where t.start_station_id is not null
      and trim(t.start_station_id) != ''

    group by
        year(t.started_at),
        month(t.started_at),
        coalesce(
            m.canonical_station_key,
            t.start_station_id
        )
),

arrivals as (

    select
        year(t.ended_at) as ride_year,
        month(t.ended_at) as ride_month,

        coalesce(
            m.canonical_station_key,
            t.end_station_id
        ) as station_key,

        max(
            coalesce(
                m.canonical_station_name,
                t.end_station_name
            )
        ) as station_name,

        round(avg(t.end_lat), 6) as station_lat,
        round(avg(t.end_lng), 6) as station_lng,

        count(*) as arrivals

    from {{ source('silver', 'trips') }} t

    left join station_mapping m
        on t.end_station_id = m.station_id

    where t.end_station_id is not null
      and trim(t.end_station_id) != ''

    group by
        year(t.ended_at),
        month(t.ended_at),
        coalesce(
            m.canonical_station_key,
            t.end_station_id
        )
),

station_flow as (

    select
        coalesce(d.ride_year, a.ride_year) as ride_year,
        coalesce(d.ride_month, a.ride_month) as ride_month,
        coalesce(d.station_key, a.station_key) as station_key,

        coalesce(d.station_name, a.station_name) as station_name,
        coalesce(d.station_lat, a.station_lat) as station_lat,
        coalesce(d.station_lng, a.station_lng) as station_lng,

        coalesce(d.departures, 0) as departures,
        coalesce(a.arrivals, 0) as arrivals,

        coalesce(d.departures, 0)
            + coalesce(a.arrivals, 0) as total_station_activity,

        coalesce(a.arrivals, 0)
            - coalesce(d.departures, 0) as net_flow

    from departures d

    full outer join arrivals a
        on d.ride_year = a.ride_year
        and d.ride_month = a.ride_month
        and d.station_key = a.station_key
)

select
    concat(
        cast(ride_year as string),
        '|',
        lpad(cast(ride_month as string), 2, '0'),
        '|',
        station_key
    ) as station_month_key,

    ride_year,
    ride_month,
    station_key,
    station_name,
    station_lat,
    station_lng,
    departures,
    arrivals,
    total_station_activity,
    net_flow

from station_flow