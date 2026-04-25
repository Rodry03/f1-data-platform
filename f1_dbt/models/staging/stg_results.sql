with source as (
    select * from {{ source('f1_raw', 'raw_results') }}
),

renamed as (
    select
        year                                    as season_year,
        round                                   as round_number,
        year || '-' || lpad(cast(round as varchar), 2, '0') as race_id,
        driver_number,
        driver_code,
        full_name                               as driver_name,
        team                                    as team_name,
        cast(position as integer)               as finish_position,
        cast(points as double)                  as points_scored,
        status
    from source
)

select * from renamed