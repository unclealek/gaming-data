
{{ macro1() }}

with source as (

    select * from {{ source('bakehouse', 'sales_transactions') }}

)

select transactionID, customerID, product, quantity, unitPrice, totalPrice
from source
