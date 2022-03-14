CREATE TABLE IF NOT EXISTS past_sale (

    past_sale_id  VARCHAR NOT NULL,
    ds DATE NOT NULL,
    realtor_id VARCHAR NOT NULL,
    created_ts TIMESTAMP,
    last_updated_ts TIMESTAMP,
    past_sale_name VARCHAR,
    sale_ts TIMESTAMP,
    item_type VARCHAR,
    PRIMARY KEY (ds, past_sale_id)


);