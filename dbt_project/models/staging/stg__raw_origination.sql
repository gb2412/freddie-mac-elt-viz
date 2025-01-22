
{{ config(
    materialized='table'
) }}


WITH stg_origination AS (
    
    SELECT
        loan_num,
        first_payment_month,
        year,
        quarter,
        loan_term,
        maturity_date,
        amortization_type,
        int_rate,
        NULLIF(loan_purpose, '9') AS loan_purpose,
        NULLIF(credit_score, 9999) AS credit_score,
        upb,
        NULLIF(ltv, 999) AS ltv,
        NULLIF(cltv, 999) AS cltv,
        NULLIF(dti, 999) AS dti,
        NULLIF(first_time_home_buyer_flag, '9') AS first_time_home_buyer_flag,
        NULLIF(zip_code, '00') AS zip_code,
        metropolitan_area,
        property_state,
        NULLIF(property_type, '99') AS property_type,
        NULLIF(occupancy_status, '9') AS occupancy_status,
        NULLIF(num_units, 99) AS num_units,
        CASE prop_val_method
            WHEN 9 THEN NULL
            WHEN 2 THEN 'Full Appr.'
            WHEN 3 THEN 'Other Appr.'
            WHEN 1 THEN 'ACE'
            WHEN 4 THEN 'ACE+PDR'
            ELSE NULL
        END AS prop_val_method,
        NULLIF(num_borrowers, 99) AS num_borrowers,
        NULLIF(channel, '9') AS channel,
        seller_name,
        servicer_name,
        NULLIF(mi_percentage, 999) AS mi_percentage,
        CASE 
            WHEN mi_canc_indicator IN ('9', '7') THEN NULL 
            ELSE mi_canc_indicator 
        END AS mi_canc_indicator,
        prepay_penalty,
        int_only_indicator,
        COALESCE(sup_conf_flag, 'N') AS sup_conf_flag,
        program_indicator,
        COALESCE(ref_indicator, 'N') AS ref_indicator,
        pre_ref_loan_num,
        date

    FROM
        {{ source('capstone_sources', 'raw_mortgage_origination')}}
)

SELECT * FROM stg_origination