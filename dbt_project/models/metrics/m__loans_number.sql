
WITH loans_number AS (

    SELECT
        month_reporting,
        COUNT( DISTINCT loan_num ) as total_loans
    FROM
        {{ ref('stg__raw_performance') }}
    GROUP BY
        month_reporting
)

SELECT
    * 
FROM loans_number