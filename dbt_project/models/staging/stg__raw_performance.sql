
{{ config(
    materialized='table'
) }}


WITH stg_performance AS (

    SELECT
        loan_num,
        LAST_DAY_OF_MONTH(
            DATE_PARSE(SUBSTR(month_reporting, 1, 4) || '-' || SUBSTR(month_reporting, 5, 2) || '-01', '%Y-%m-%d')
        ) 
        AS month_reporting,
        year,
        quarter,
        remaining_months_to_legal_maturity,
        loan_age,
        current_actual_upb,
        current_delinquency_status,
        ddlpi,
        current_int_rate,
        NULLIF(eltv, 999) as eltv,
        curr_int_upb,
        current_non_int_upb, 
        COALESCE(modification_flag, 'N') as modification_flag,
        step_modification_flag,
        curr_month_modification_cost,
        cum_modification_cost,
        borrower_assistance_status_code,
        payment_deferral,
        disaster_delinq_flag,
        zero_balance_code,
        zero_balance_effective_date,
        defect_settlement_date,
        zero_bal_removal_upb,
        delinquent_accrued_int,
        actual_loss,
        mi_recoveries,
        non_mi_recoveries,
        net_sale_proceeds,
        total_expenses,
        legal_costs,
        maintenance_and_preservation_costs,
        taxes_and_insurance,
        miscellaneous_expenses,
        date

    FROM
        {{ source('capstone_sources', 'raw_mortgage_performance')}}
)

SELECT * FROM stg_performance