with source as (
    select * from {{ source('f1_raw', 'raw_fastest_laps') }}
),

renamed as (
    select
        year                                    as season_year,
        round                                   as round_number,
        year || '-' || lpad(cast(round as varchar), 2, '0') as race_id,
        event_name,
        driver_code,
        team                                    as team_name,
        round(lap_time_seconds, 3)              as lap_time_seconds
    from source
)

select * from renamed