{{ config(
    materialized='incremental',
    unique_key='track_status_sk'
) }}

with source as (
    select * from {{ source('f1_raw', 'raw_track_status') }}

    {% if is_incremental() %}
        where year || '-' || lpad(cast(round as varchar), 2, '0')
              not in (select distinct race_id from {{ this }})
    {% endif %}
),

renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['year', 'round', 'time']) }} as track_status_sk,
        year                                    as season_year,
        round                                   as round_number,
        year || '-' || lpad(cast(round as varchar), 2, '0') as race_id,
        cast(seq as integer)                    as source_seq,
        round(epoch(cast(time as interval)), 3) as time_s,
        cast(status as integer)                 as status_code,
        case cast(status as integer)
            when 1 then 'green'
            when 2 then 'yellow'
            when 4 then 'safety_car'
            when 5 then 'red_flag'
            when 6 then 'vsc_deployed'
            when 7 then 'vsc_ending'
        end                                     as status_label,
        message,
        round(epoch(cast(session_end_time as interval)), 3) as session_end_s
    from source
),

-- Estados posteriores al fin de sesión no abren periodo (duración <= 0).
-- Con timestamps duplicados en una carrera, gana el último en orden de origen.
cleaned as (
    select *
    from renamed
    where time_s < session_end_s
    qualify row_number() over (
        partition by race_id, time_s
        order by source_seq desc
    ) = 1
)

select * from cleaned
