{% snapshot driver_team_snapshot %}

{{ config(
    target_schema='snapshots',
    unique_key='driver_team_sk',
    strategy='check',
    check_cols=['team_name']
) }}

select
    {{ dbt_utils.generate_surrogate_key(['driver_code', 'season_year']) }} as driver_team_sk,
    driver_code,
    driver_name,
    season_year,
    team_name
from {{ ref('stg_results') }}
GROUP BY driver_code, driver_name, team_name, season_year 


{% endsnapshot %}