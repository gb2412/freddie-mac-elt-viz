
{% test accepted_values_with_null(model, column_name, values) %}
    WITH validation AS (
        SELECT
            {{ column_name }} AS value
        FROM {{ model }}
    )
    SELECT *
    FROM validation
    WHERE value IS NOT NULL AND value NOT IN ({{ values | join(', ') }})
{% endtest %}