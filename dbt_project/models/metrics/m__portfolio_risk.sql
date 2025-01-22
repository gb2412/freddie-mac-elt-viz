
WITH portfolio_default_probability AS (

    SELECT
        ad.month_reporting,
        AVG(ad.curr_default) AS current_default_rate,
        AVG(ad.ever_bad_one_year_default) AS ever_bad_one_year_default_rate,
        AVG(mp.prediction) AS predicted_ever_bad_one_year_default_rate

    FROM
        {{ ref('int__actual_defaults') }} ad
    JOIN
        {{ ref('int__ml_model_predictions') }} mp
        ON ad.loan_num = mp.loan_num AND ad.month_reporting = mp.month_reporting
    GROUP BY
        ad.month_reporting
)

SELECT 
    *
FROM portfolio_default_probability