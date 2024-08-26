-- To retain the table if some logic mistake occur while updating this approac may be used through snowflake

CREATE OR REPLACE TABLE dim_product_cloned AS
SELECT *
FROM dim_product 
AT (TIMESTAMP => CURRENT_TIMESTAMP() - INTERVAL '120 MINUTES');



CREATE OR REPLACE TABLE dim_product AS
SELECT * FROM dim_product_cloned;

WITH ranked_products AS (
    SELECT 
        product_id,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_id) AS rn
    FROM 
        dim_product
)
select * from ranked_products;

drop table dim_product_cloned;
