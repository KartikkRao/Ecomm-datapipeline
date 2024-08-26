CREATE STORAGE INTEGRATION ecomm_external_storage
    type = external_stage
    storage_provider = s3
    storage_aws_role_arn = 'Your_key'
    enabled = true
    storage_allowed_locations = ('Your_storage_location')

-- DESC INTEGRATION ecomm_external_storage;

CREATE OR REPLACE FILE FORMAT file_format_csv
    type = 'csv'
    field_delimiter = ','
    skip_header = 1
    null_if = ('NULL','null')
    empty_field_as_null = True
    field_optionally_enclosed_by = '"';

CREATE OR REPLACE STAGE ecomm_data_stage
    url = 'Your_storage_location'
    file_format = file_format_csv
    storage_integration = ecomm_external_storage;

CREATE TABLE ecomm_transformed(
    event_time timestamp,
    event_type varchar(50),
    product_id number(20,0),
    category_id number,
    brand varchar(20),
    price float,
    user_id number, -- default (38,0)
    user_session string
);

COPY INTO ecomm_transformed(
     event_time,
     event_type,
     product_id,
     category_id,
     brand,
     price,
     user_id,
     user_session
)FROM @ecomm_data_stage/ecomm_2024-08-21_09-18-03/

SELECT * FROM ecomm_transformed LIMIT 10;

UPDATE ecomm_transformed
SET brand = COALESCE(brand, 'other');   
     
CREATE OR REPLACE PIPE ecomm_transformed_data
    auto_ingest = True
    AS
    COPY INTO ecomm_transformed(
     event_time,
     event_type,
     product_id,
     category_id,
     brand,
     price,
     user_id,
     user_session
)FROM @ecomm_data_stage;

-- DESC PIPE ecomm_transformed_data;

--SELECT count(*) FROM ecomm_transformed;


CREATE OR REPLACE TABLE dim_event(
    event_id NUMBER AUTOINCREMENT,
    event_type VARCHAR(50),
    event_time timestamp,
    year NUMBER,
    month NUMBER,
    day NUMBER,
    day_of_week NUMBER
);

INSERT INTO dim_event (event_type, event_time, year, month, day, day_of_week)
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



CREATE OR REPLACE TABLE dim_product AS (
    SELECT DISTINCT
        product_id 
    FROM 
        ecomm_transformed
);

--  SELECT COUNT(*) FROM dim_product;




CREATE OR REPLACE TABLE dim_category AS (
    SELECT DISTINCT
        category_id 
    FROM 
        ecomm_transformed
);

--  SELECT COUNT(*) FROM dim_category;




CREATE OR REPLACE TABLE dim_brand (
    brand_id NUMBER AUTOINCREMENT,
    brand VARCHAR(50)
);

INSERT INTO dim_brand (brand)
SELECT DISTINCT brand
FROM ecomm_transformed;

-- SELECT COUNT(*) FROM dim_brand;




CREATE OR REPLACE TABLE dim_user(
    user_sk NUMBER autoincrement,
    user_id NUMBER,
    user_session STRING
);

INSERT INTO dim_user (user_id , user_session)
SELECT DISTINCT user_id , user_session
FROM ecomm_transformed;

-- SELECT COUNT(*) FROM dim_user;




CREATE OR REPLACE TABLE fact_price AS (
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

-- SELECT count(*) FROM fact_price ;
-- SELECT * FROM fact_price LIMIT 50;

