{% test unique_combination(model, column_name, combination_of_columns) %}

with validation as (
    select
        {{ combination_of_columns | join(', ') }},
        count(*) as row_count
    from {{ model }}
    group by {{ combination_of_columns | join(', ') }}
    having count(*) > 1
)

select * from validation

{% endtest %}