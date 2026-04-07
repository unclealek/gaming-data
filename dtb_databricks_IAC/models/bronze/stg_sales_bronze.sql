{{ config(materialized='incremental', unique_key='user_id', incremental_strategy='merge') }}

with raw_source as (
    select * from {{ source('sql_db', 'sales_bronze') }}
),

prepared as (
    select
        cast(user_id as double) as user_id,
        trim(lower(first_name)) as first_name_raw,
        trim(lower(last_name)) as last_name_raw,
        {{ clean_email_chars('email_addr') }} as email_mid,
        trim(lower(region_name)) as region_name,
        cast(sale_date as date) as sale_date,
        least(base_price, 1000) as base_price,
        jan_sales,
        feb_sales,
        least(greatest(user_rating, 0), 5) as user_rating,
        batch_id,
        ingestion_timestamp
    from raw_source
),

logic_fixes as (
    select
        *,
        coalesce(first_name_raw, split_part(email_mid, last_name_raw, 1)) as first_name,
        coalesce(last_name_raw, nullif(split_part(split_part(email_mid, '@', 1), first_name_raw, 2), '')) as last_name
    from prepared
),


final_prep as (
    select
        *,
        case
            when email_mid not like '%@example.com%' then
                concat(regexp_replace(email_mid, '(@example|.com|@.com)$', ''), '@example.com')
            else email_mid
        end as email_addr
    from logic_fixes
),

deduplicated as (
    select * from (
        select *,
            row_number() over (
                partition by user_id, first_name, last_name, email_addr, region_name, sale_date, base_price, jan_sales, feb_sales, user_rating
                order by ingestion_timestamp desc
            ) as row_num
        from final_prep
    ) where row_num = 1
)

select
    user_id,
    first_name,
    last_name,
    email_addr,
    region_name,
    sale_date,
    base_price,
    jan_sales,
    feb_sales,
    user_rating,
    batch_id,
    ingestion_timestamp
from deduplicated
