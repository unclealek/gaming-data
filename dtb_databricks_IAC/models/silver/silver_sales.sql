{{ config(
    materialized='incremental',
    unique_key='user_id',
    incremental_strategy='merge'
) }}

with bronze_data as (
    select * from {{ ref('stg_sales_bronze') }}
    {% if is_incremental() %}
      where ingestion_timestamp > (select max(processed_at) from {{ this }})
    {% endif %}
),

deduplicated as (
    select * from (
        select *,
            row_number() over (
                partition by user_id
                order by ingestion_timestamp desc, batch_id desc
            ) as final_row_num
        from bronze_data
    ) where final_row_num = 1
),

logic_layer as (
    select
        user_id,
        first_name,
        last_name,
        {{ null_email('email_addr', 'first_name', 'last_name') }} as email_with_nulls_fixed,
        region_name,
        sale_date,
        base_price,
        jan_sales,
        feb_sales,
        (coalesce(jan_sales, 0) + coalesce(feb_sales, 0)) as total_q1_sales,
        user_rating,
        batch_id,
        current_timestamp() as processed_at
    from deduplicated
),

final_polishing as (
    select
        *,
        {{ fix_double_at('email_with_nulls_fixed') }} as email_addr
    from logic_layer
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
    total_q1_sales,
    user_rating,
    batch_id,
    processed_at
from final_polishing
-- Use IS NOT NULL for SQL filtering
where first_name is not null
  and last_name is not null
  and user_id is not null
