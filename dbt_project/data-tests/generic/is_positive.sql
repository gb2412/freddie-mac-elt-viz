{% test is_positive(model, column_name) %}

with validation as (

    select
        {{ column_name }} as positive_field

    from {{ model }}

),

validation_errors as (

    select
        positive_field

    from validation
    -- if this is true, then positive_field is actually non-positive!
    where positive_field <= 0

)

select *
from validation_errors

{% endtest %}

{% test is_not_negative(model, column_name) %}

with validation as (

    select
        {{ column_name }} as positive_field

    from {{ model }}

),

validation_errors as (

    select
        positive_field

    from validation
    -- if this is true, then positive_field is actually negative!
    where positive_field < 0

)

select *
from validation_errors

{% endtest %}