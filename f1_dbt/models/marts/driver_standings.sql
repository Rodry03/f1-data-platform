with results as (
    select * from {{ ref('stg_results') }}
),

standings as (
    select
        season_year,
        driver_code,
        driver_name,
        team_name,
        count(*)                                                as races_entered,
        sum(points_scored)                                      as total_points,
        count(case when finish_position = 1 then 1 end)         as wins,
        count(case when finish_position <= 3 then 1 end)        as podiums,
        count(case when finish_position <= 10 then 1 end)       as points_finishes,
        count(case when status = 'Finished' then 1 end)         as races_finished,
        round(avg(finish_position), 2)                          as avg_finish_position,
        min(finish_position)                                    as best_finish,
        {{ classify_position('min(finish_position)') }}         as best_finish_label
    from results
    where finish_position is not null
    group by season_year, driver_code, driver_name, team_name
),

ranked as (
    select
        *,
        row_number() over (
            partition by season_year
            order by total_points desc
        ) as championship_position
    from standings
)

select * from ranked
order by season_year, championship_position