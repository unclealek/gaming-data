
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table',tags=['model1']) }}

with source as (

    select * from {{ source('bakehouse', 'sales_customers') }}

)

select customerID, first_name, last_name, email_address, gender
from source

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
