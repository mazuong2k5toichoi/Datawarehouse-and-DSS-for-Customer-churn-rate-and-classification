{% macro safe_divide(numerator, denominator) %}
(
    case
        when {{ denominator }}::numeric is null or {{ denominator }}::numeric = 0
        then null
        else {{ numerator }}::numeric / {{ denominator }}::numeric
    end
)
{% endmacro %}
