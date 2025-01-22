
-- setting prediction start quarter
{% set prediction_start_quarter = '2021-01-31' %}

WITH filtered_performance AS (
    SELECT
        loan_num,
        month_reporting,
        current_actual_upb,
        borrower_assistance_status_code,
        payment_deferral,
        current_delinquency_status,
        zero_balance_code
    FROM
        {{ ref('stg__raw_performance') }}
    WHERE
        {{ is_default('current_delinquency_status', 'zero_balance_code') }} = 0
        AND 
            month_reporting >= DATE('{{ prediction_start_quarter }}')
),

filtered_origination AS (
    SELECT
        loan_num,
        prepay_penalty,
        cltv,
        credit_score,
        num_borrowers,
        upb
    FROM
        {{ ref('stg__raw_origination') }}
),

ml_model_features AS (

    SELECT
        o.loan_num AS loan_num,
        p.month_reporting AS month_reporting,
        p.borrower_assistance_status_code AS assistance_status,
        p.payment_deferral AS payment_deferral,
        o.prepay_penalty AS prepay_penalty,
        o.cltv AS cltv,
        o.credit_score AS credit_score,
        CAST(p.current_delinquency_status AS int) AS delinquency_status,
        o.num_borrowers AS num_borrowers,
        {{ payment_ratio('o.upb', 'p.current_actual_upb') }} AS payment_ratio,
        MAX(CAST(p.current_delinquency_status AS int)) OVER (
                PARTITION BY p.loan_num 
                ORDER BY p.month_reporting
                ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        ) AS max_delinquency_status_yoy,
        SUM(CASE WHEN p.current_delinquency_status = '2' THEN 1 ELSE 0 END) OVER (
            PARTITION BY p.loan_num 
            ORDER BY p.month_reporting
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        ) AS delinquency_status_2_count_yoy

    FROM
        filtered_performance p 
    JOIN
        filtered_origination o
        ON p.loan_num = o.loan_num
),

ml_model AS (

    SELECT
        loan_num,
        month_reporting,
        -2.82531915031764 as intercept,
        CASE WHEN assistance_status = 'R' THEN 1.1300736179557895 ELSE 0 END AS assistance_status__repayment,
        CASE WHEN payment_deferral = 'Y' THEN 0.7102330370749436 ELSE 0 END AS payment_deferral__Y,
        CASE WHEN prepay_penalty = 'Y' THEN 0.2445560085700683 ELSE 0 END AS prepay_penalty__Y,
        CASE WHEN cltv IS NULL THEN 0.6243996910665761 ELSE 0 END AS cltv__NULL,
        CASE WHEN credit_score <= 700 THEN 0.342532931496905 ELSE 0 END AS credit_score__lt_700,
        CASE WHEN credit_score <= 750 THEN 0.30011853258974325 ELSE 0 END AS credit_score__lt_750,
        CASE WHEN credit_score <= 800 THEN 0.6754464719390741 ELSE 0 END AS credit_score__lt_800,
        CASE WHEN payment_ratio <= 0.04 THEN 0.2275324032429015 ELSE 0 END AS payment_ratio__lt_0_04,
        CASE WHEN payment_ratio <= 0.06 THEN 0.24891666097736245 ELSE 0 END AS payment_ratio__lt_0_06,
        CASE WHEN payment_ratio <= 0.11 THEN 0.453543993793661 ELSE 0 END AS payment_ratio__lt_0_11,
        CASE WHEN delinquency_status <= 1 THEN -1.7910298493949692 ELSE 0 END AS delinquency_status__lt_1,
        CASE WHEN num_borrowers <= 2 THEN 0.6485398699883044 ELSE 0 END AS num_borrowers__lt_2,
        CASE WHEN max_delinquency_status_yoy <= 2 THEN -1.86271968782344 ELSE 0 END AS max_delinquency_status_yoy__lt_2,
        CASE WHEN delinquency_status_2_count_yoy <= 1 THEN -0.5157846201670598 ELSE 0 END AS delinquency_status_2_count_yoy__lt_1

    FROM
        ml_model_features
),

predictions AS (

    SELECT
        loan_num,
        month_reporting,
        1 / (1 + EXP(-(
            intercept +
            assistance_status__repayment +
            payment_deferral__Y +
            prepay_penalty__Y +
            cltv__NULL +
            credit_score__lt_700 +
            credit_score__lt_750 +
            credit_score__lt_800 +
            payment_ratio__lt_0_04 +
            payment_ratio__lt_0_06 +
            payment_ratio__lt_0_11 +
            delinquency_status__lt_1 +
            num_borrowers__lt_2 +
            max_delinquency_status_yoy__lt_2 +
            delinquency_status_2_count_yoy__lt_1
        ))) AS prediction
    FROM
        ml_model

)


SELECT
    *
FROM predictions
