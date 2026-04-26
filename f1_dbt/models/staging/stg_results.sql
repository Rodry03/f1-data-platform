{{ config(
    materialized='incremental',
    unique_key='result_sk'
) }}

with source as (
    select * from {{ source('f1_raw', 'raw_results') }}

    {% if is_incremental() %}
        where year > (select max(season_year) from {{ this }})
    {% endif %}
),

renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['year', 'round', 'driver_code']) }} as result_sk,
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