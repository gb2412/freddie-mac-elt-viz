
{% test is_valid_probability(model, column_name) %}
    WITH validation AS (
        SELECT
            {{ column_name }} AS value
        FROM {{ model }}
    )
    SELECT *
    FROM validation
    WHERE value < 0 OR value > 1
{% endtest %}