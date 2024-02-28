CREATE TABLE IF NOT EXISTS olist_missed_deadline_tb(
    id SERIAL,
    order_id VARCHAR(200),
    seller_id VARCHAR(200),
    shipping_limit_date DATE,
    price FLOAT,
    freight_value FLOAT,
    product_id VARCHAR(200),
    product_category_name VARCHAR(200),
    customer_id VARCHAR(200),
    order_status VARCHAR(200),
    order_purchase_timestamp DATE,
    order_delivered_carrier_date DATE,
    order_delivered_customer_date DATE,
    order_estimated_delivery_date DATE,
    PRIMARY KEY (id, order_id, seller_id, product_id, customer_id)
);
