USE DATABASE ecomm_data;
USE SCHEMA ecomm;



CREATE OR REPLACE TABLE staging_dim_event(
    event_id NUMBER AUTOINCREMENT,
    event_type VARCHAR(50),
    event_time timestamp,
    year NUMBER,
    month NUMBER,
    day NUMBER,
    day_of_week NUMBER
);

INSERT INTO staging_dim_event (event_type, event_time, year, month, day, day_of_week)
SELECT DISTINCT
    event_type,
    event_time,
    EXTRACT(YEAR FROM event_time) AS year,
    EXTRACT(MONTH FROM event_time) AS month,
    EXTRACT(DAY FROM event_time) AS day,
    EXTRACT(DOW FROM event_time) AS day_of_week
FROM 
    ecomm_transformed;
-- SELECT COUNT(*) FROM dim_event ;
-- SELECT COUNT(*) FROM staging_dim_event;





MERGE INTO dim_event as e
USING staging_dim_event as se
ON e.event_type = se.event_type AND e.event_time = se.event_time
WHEN NOT MATCHED THEN
INSERT (event_type, event_time, year, month, day, day_of_week)
VALUES (se.event_type, se.event_time, se.year, se.month, se.day, se.day_of_week);


CREATE OR REPLACE TABLE staging_dim_product(
    product_id number(20,0)
);

INSERT INTO staging_dim_product (product_id)
    SELECT DISTINCT 
        product_id
    FROM
        ecomm_transformed;

MERGE INTO dim_product as p
USING staging_dim_product as sp
ON p.product_id = sp.product_id
WHEN NOT MATCHED THEN
INSERT (product_id)
VALUES (sp.product_id);





CREATE OR REPLACE TABLE staging_dim_category(
    category_id number
);

INSERT INTO staging_dim_category (category_id)
    SELECT DISTINCT 
        category_id
    FROM
        ecomm_transformed;

MERGE INTO dim_category as c
USING staging_dim_category as sc
ON c.category_id = sc.category_id
WHEN NOT MATCHED THEN
INSERT (category_id)
VALUES (sc.category_id);






CREATE OR REPLACE TABLE staging_dim_brand (
    brand_id NUMBER AUTOINCREMENT,
    brand VARCHAR(50)
);

INSERT INTO staging_dim_brand (brand)
SELECT DISTINCT brand
FROM ecomm_transformed;

MERGE INTO dim_brand as b
USING staging_dim_brand as sb
ON b.brand = sb.brand
WHEN NOT MATCHED THEN
INSERT (brand)
VALUES (sb.brand);






CREATE OR REPLACE TABLE staging_dim_user(
    user_sk NUMBER autoincrement,
    user_id NUMBER,
    user_session STRING
);

INSERT INTO staging_dim_user (user_id , user_session)
SELECT DISTINCT user_id , user_session
FROM ecomm_transformed;

MERGE INTO dim_user as u
USING staging_dim_user as su
ON u.user_id = su.user_id AND u.user_session = su.user_session
WHEN NOT MATCHED THEN
INSERT (user_id , user_session)
VALUES (su.user_id , su.user_session);






CREATE OR REPLACE TABLE staging_fact_price AS (
    SELECT 
        ev.event_id,
        e.product_id,
        e.category_id,
        b.brand_id,
        u.user_sk,
        e.price
    FROM 
        ecomm_transformed AS e 
    INNER JOIN 
        dim_brand as b ON e.brand = b.brand
    INNER JOIN 
        dim_event as ev ON e.event_time = ev.event_time AND e.event_type = ev.event_type
    INNER JOIN
        dim_user as u ON e.user_id = u.user_id AND e.user_session = u.user_session
);

MERGE INTO fact_price as fp
USING staging_fact_price as sfp
ON 
    fp.event_id = sfp.event_id AND
    fp.product_id = sfp.product_id AND 
    fp.category_id = sfp.category_id AND 
    fp.brand_id = sfp.brand_id AND 
    fp.user_sk = sfp.user_sk AND 
    fp.price = sfp.price
WHEN NOT MATCHED THEN 
INSERT (event_id , product_id , category_id , brand_id , user_sk , price)
VALUES (sfp.event_id , sfp.product_id , sfp.category_id , sfp.brand_id , sfp.user_sk , sfp.price);




  
drop table staging_dim_event;
drop table staging_dim_product;
drop table staging_dim_category;
drop table staging_dim_brand;
drop table staging_dim_user;
drop table staging_fact_price;
truncate table ecomm_transformed;

--select count(*) from fact_price ;
--select count(*) from ecomm_transformed;
