with source as (
    select * from {{ source('f1_raw', 'raw_races') }}
),

renamed as (
    select
        year                                    as season_year,
        round                                   as round_number,
        event_name,
        country,
        cast(date as date)                      as race_date,
        year || '-' || lpad(cast(round as varchar), 2, '0') as race_id
    from source
)

select * from renamed