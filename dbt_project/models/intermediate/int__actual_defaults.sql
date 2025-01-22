
WITH default_conditions AS (

    SELECT
        loan_num,
        month_reporting,
        MAX(month_reporting) OVER () AS most_recent_month,
        {{ is_default('current_delinquency_status', 'zero_balance_code') }} AS curr_default
    FROM 
        {{ ref('stg__raw_performance') }}
),

default_following_year AS (

    SELECT
        loan_num,
        month_reporting,
        curr_default,
        CASE
            WHEN month_reporting > DATE_ADD('month', -12, most_recent_month)
                THEN NULL 
            ELSE MAX(curr_default) OVER (
                    PARTITION BY loan_num 
                    ORDER BY month_reporting 
                    ROWS BETWEEN 1 FOLLOWING AND 12 FOLLOWING
                )
        END AS ever_bad_one_year_default
    FROM default_conditions
)

SELECT
    *
FROM default_following_year