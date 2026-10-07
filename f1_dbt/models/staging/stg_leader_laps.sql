{{ config(
    materialized='incremental',
    unique_key='leader_lap_sk'
) }}

with source as (
    select * from {{ source('f1_raw', 'raw_leader_laps') }}

    {% if is_incremental() %}
        where year || '-' || lpad(cast(round as varchar), 2, '0')
              not in (select distinct race_id from {{ this }})
    {% endif %}
),

renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['year', 'round', 'lap_number']) }} as leader_lap_sk,
        year                                    as season_year,
        round                                   as round_number,
        year || '-' || lpad(cast(round as varchar), 2, '0') as race_id,
        cast(lap_number as integer)             as lap_number,
        driver_code,
        round(epoch(cast(lap_start_time as interval)), 3) as lap_start_s,
        round(epoch(cast(lap_end_time as interval)), 3)   as raw_lap_end_s
    from source
),

-- Rangos contiguos: cada vuelta termina cuando empieza la siguiente.
-- No vale el Time de la propia vuelta: con bandera roja (p. ej. Mónaco 2024)
-- el Time de la vuelta 1 es posterior al inicio de la vuelta 2.
-- La última vuelta cierra con su Time.
contiguous as (
    select
        leader_lap_sk,
        season_year,
        round_number,
        race_id,
        lap_number,
        driver_code,
        lap_start_s,
        coalesce(
            lead(lap_start_s) over (partition by race_id order by lap_number),
            raw_lap_end_s
        )                                       as lap_end_s
    from renamed
)

select * from contiguous
