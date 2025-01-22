
WITH previous_month_cte AS (

    SELECT
        loan_num,
        month_reporting,
        COALESCE(LAG(current_int_rate) OVER (
            PARTITION BY loan_num 
            ORDER BY month_reporting), current_int_rate)
            AS previous_month_int_rate,
        COALESCE(LAG(curr_int_upb) OVER (
            PARTITION BY loan_num 
            ORDER BY month_reporting), curr_int_upb)
            AS previous_month_int_bearing_upb,
        current_delinquency_status,
        COALESCE(LAG(current_delinquency_status) OVER (
            PARTITION BY loan_num 
            ORDER BY month_reporting), '0')
            AS previous_month_delinquency_status,
        remaining_months_to_legal_maturity AS months_to_maturity
    FROM
        {{ ref('stg__raw_performance') }}
    -- exclude disposed loans
    WHERE 
        current_delinquency_status != 'RA'

),

current_month_interest_cte AS (

    SELECT
        loan_num,
        month_reporting,
        {{ monthly_interest('previous_month_int_bearing_upb', 'previous_month_int_rate') }} 
            AS current_month_interest,
        CAST(current_delinquency_status AS int) 
            AS current_delinquency_status,
        CAST(previous_month_delinquency_status AS int) 
            AS previous_month_delinquency_status
    FROM previous_month_cte

),

paid_interest_cte AS (

    SELECT
        month_reporting,
        CASE
            WHEN current_delinquency_status > previous_month_delinquency_status
                THEN 0
            ELSE 
                SUM(current_month_interest) OVER (
                    PARTITION BY loan_num
                    ORDER BY month_reporting
                    ROWS BETWEEN
                        previous_month_delinquency_status PRECEDING 
                            AND current_delinquency_status PRECEDING
                    )
        END AS paid_interest
    FROM current_month_interest_cte

),

aggregate_monthly_interest_cte AS (
    
    SELECT 
        month_reporting,
        SUM(paid_interest) AS total_monthly_interest
    FROM paid_interest_cte
    GROUP BY month_reporting
       
)

SELECT
    *
FROM aggregate_monthly_interest_cte
ORDER BY month_reporting