with track_status as (
    select * from {{ ref('stg_track_status') }}
),

leader_laps as (
    select * from {{ ref('stg_leader_laps') }}
),

races as (
    select * from {{ ref('stg_races') }}
),

-- Cada cambio de estado abre un periodo que dura hasta el siguiente cambio;
-- el último periodo cierra con el fin de sesión
periods as (
    select
        race_id,
        season_year,
        round_number,
        row_number() over (
            partition by race_id
            order by time_s
        )                                                   as period_number,
        status_code,
        status_label,
        message,
        time_s                                              as start_time_s,
        coalesce(
            lead(time_s) over (partition by race_id order by time_s),
            session_end_s
        )                                                   as end_time_s
    from track_status
),

lap_bounds as (
    select
        race_id,
        min(lap_start_s)                                    as race_start_s,
        max(lap_number)                                     as last_lap
    from leader_laps
    group by race_id
),

-- Vuelta del líder en curso al empezar y al terminar cada periodo.
-- start: lap_start <= t < lap_end ; end: lap_start < t <= lap_end
-- (un periodo que acaba justo en la línea pertenece a la vuelta que se cierra)
with_laps as (
    select
        p.*,
        case
            when p.start_time_s < b.race_start_s then 0
            else coalesce(ls.lap_number, b.last_lap)
        end                                                 as start_lap,
        case
            when p.end_time_s <= b.race_start_s then 0
            else coalesce(le.lap_number, b.last_lap)
        end                                                 as end_lap
    from periods p
    left join lap_bounds b
        on p.race_id = b.race_id
    left join leader_laps ls
        on  p.race_id = ls.race_id
        and p.start_time_s >= ls.lap_start_s
        and p.start_time_s <  ls.lap_end_s
    left join leader_laps le
        on  p.race_id = le.race_id
        and p.end_time_s >  le.lap_start_s
        and p.end_time_s <= le.lap_end_s
),

-- Neutralización anterior (ignorando green/yellow) de cada periodo neutralizado
neutralisations as (
    select
        race_id,
        period_number,
        lag(status_label) over w                            as prev_neutral_label,
        lag(end_lap) over w                                 as prev_neutral_end_lap
    from with_laps
    where status_label not in ('green', 'yellow')
    window w as (partition by race_id order by period_number)
),

-- Heurística: un SC que sigue directamente a una bandera roja y empieza a <= 2
-- vueltas de ella es el procedimiento de reanudación, no un SC por incidente
flagged as (
    select
        w.*,
        coalesce(
            w.status_label = 'safety_car'
            and n.prev_neutral_label = 'red_flag'
            and w.start_lap - n.prev_neutral_end_lap <= 2,
            false
        )                                                   as is_restart_procedure
    from with_laps w
    left join neutralisations n
        on  w.race_id = n.race_id
        and w.period_number = n.period_number
)

select
    w.race_id,
    w.season_year,
    w.round_number,
    r.event_name,
    w.period_number,
    w.status_code,
    w.status_label,
    w.message,
    w.start_time_s,
    w.end_time_s,
    round(w.end_time_s - w.start_time_s, 3)                 as duration_s,
    w.start_lap,
    w.end_lap,
    w.is_restart_procedure
from flagged w
left join races r
    on w.race_id = r.race_id
order by w.season_year, w.round_number, w.period_number
