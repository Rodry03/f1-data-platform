{% macro format_lap_time(seconds) %}
    cast(floor({{ seconds }} / 60) as integer) || ':' || printf('%06.3f', {{ seconds }} % 60)
{% endmacro %}