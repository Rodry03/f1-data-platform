{{ config(
    materialized='incremental',
    unique_key='race_id || driver_code'
) }}

with source as (
    select * from {{ source('f1_raw', 'raw_results') }}

    {% if is_incremental() %}
        where year > (select max(season_year) from {{ this }})
    {% endif %}
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