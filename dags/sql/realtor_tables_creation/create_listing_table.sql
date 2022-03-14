CREATE TABLE IF NOT EXISTS listing (
    listing_id VARCHAR NOT NULL,
    ds  DATE NOT NULL,
    realtor_id  VARCHAR NOT NULL,
    created_ts TIMESTAMP,
    last_updated_ts TIMESTAMP,
    listing_name VARCHAR,
    transaction_type VARCHAR,
    start_ts TIMESTAMP ,
    end_ts TIMESTAMP,
    item_type VARCHAR,
    PRIMARY KEY (ds, listing_id)
);