with fastest as (
    select * from {{ ref('stg_fastest_laps') }}
),

races as (
    select * from {{ ref('stg_races') }}
),

enriched as (
    select
        f.season_year,
        f.round_number,
        r.event_name,
        r.country,
        r.race_date,
        f.driver_code,
        f.team_name,
        f.lap_time_seconds,
        rank() over (
            partition by f.season_year
            order by f.lap_time_seconds asc
        ) as fastest_lap_rank_in_season
    from fastest f
    left join races r
        on f.race_id = r.race_id
)

select * from enriched
order by season_year, round_number