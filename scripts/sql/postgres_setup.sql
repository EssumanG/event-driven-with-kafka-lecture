-- Active: 1768996423872@@127.0.0.1@5433@realtime_db


CREATE TABLE event_log(
    event_id SERIAL PRIMARY KEY,
    event_type VARCHAR(20) CHECK (event_type IN ('product_view', 'product_purchase')),
    product_name VARCHAR(200) NOT NULL,
    customer_name VARCHAR(200) NOT NULL,
    unit_price NUMERIC(10, 2) NOT NULL,
    quantity INT,
    event_date TIMESTAMP 
);

ALTER TABLE event_log
ADD CONSTRAINT event_log_pk UNIQUE (
    event_type,
    product_name,
    customer_name,
    event_date,
    quantity
);