
WITH defaulted_loans AS (

    SELECT
        loan_num,
        month_reporting,
        actual_loss
    FROM
        {{ ref('stg__raw_performance') }}
    WHERE
        actual_loss IS NOT NULL
),

aggegate_monthly_loss AS (

    SELECT
        month_reporting,
        SUM(actual_loss) AS total_monthly_loss
    FROM
        defaulted_loans
    GROUP BY
        month_reporting
)

SELECT
    * 
FROM aggegate_monthly_loss