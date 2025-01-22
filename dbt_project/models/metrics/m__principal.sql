
WITH total_upb AS (

    SELECT
        month_reporting,
        SUM(current_actual_upb) as total_current_upb
    FROM
        {{ ref('stg__raw_performance') }}
    GROUP BY
        month_reporting
)

SELECT
    * 
FROM total_upb