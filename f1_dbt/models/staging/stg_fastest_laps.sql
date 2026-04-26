{{ config(
    materialized='incremental',
    unique_key='fastest_lap_sk'
) }}


with source as (
    select * from {{ source('f1_raw', 'raw_fastest_laps') }}

    {% if is_incremental() %}
        where year > (select max(season_year) from {{ this }})
    {% endif %}
),

renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['year', 'round', 'driver_code']) }} as fastest_lap_sk,
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


