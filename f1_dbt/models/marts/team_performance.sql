with results as (
    select * from {{ ref('stg_results') }}
),

performance as (
    select
        season_year,
        team_name,
        count(distinct driver_code)                     as drivers_count,
        count(*)                                        as total_entries,
        sum(points_scored)                              as total_points,
        count(case when finish_position = 1 then 1 end) as wins,
        count(case when finish_position <= 3 then 1 end) as podiums,
        round(avg(finish_position), 2)                  as avg_finish_position
    from results
    where finish_position is not null
    group by season_year, team_name
),

ranked as (
    select
        *,
        row_number() over (
            partition by season_year
            order by total_points desc
        ) as constructors_position
    from performance
)

select * from ranked
order by season_year, constructors_position