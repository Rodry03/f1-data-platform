{% macro classify_position(position_col) %}
    case
        when {{ position_col }} = 1 then 'Victoria'
        when {{ position_col }} <= 3 then 'Podio'
        when {{ position_col }} <= 10 then 'Puntos'
        when {{ position_col }} is not null then 'Sin puntos'
        else 'No clasificado'
    end
{% endmacro %}