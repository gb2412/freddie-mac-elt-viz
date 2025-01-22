
{% macro is_default(delinq_status, zero_bal_code) %}

    CASE
        WHEN {{ delinq_status }} NOT IN ('0','1','2') THEN 1
        WHEN {{ delinq_status }} = 'RA' THEN 1
        WHEN {{ zero_bal_code }} IN ('02','03','09','15') THEN 1
        ELSE 0
    END

{% endmacro %}


{% macro monthly_int_rate(int_rate) %}

    ({{ int_rate }} / 100) / 12

{% endmacro %}


{% macro monthly_interest(principal, annual_int_rate) %}

    {{ principal }} * {{ monthly_int_rate(annual_int_rate) }}

{% endmacro %}


{% macro monthly_payment(principal, annual_int_rate, term) %}

    ({{ principal }} * ( {{ monthly_int_rate(annual_int_rate) }} * (1 + {{ monthly_int_rate(annual_int_rate) }} ) ** {{ term }} ) ) / ( (1 + {{ monthly_int_rate(annual_int_rate) }} ) ** {{ term }} - 1 )

{% endmacro %}


{% macro payment_ratio(orig_upb, curr_upb) %}

    ({{ orig_upb }} - {{ curr_upb }}) / {{ orig_upb }}

{% endmacro %}

