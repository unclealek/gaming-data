select * from {{ ref('silver_sales') }}
where
    first_name is null
    or user_rating not between 0 and 5
    or region_name not in ('north', 'south', 'east', 'west', 'unknown', 'central')
