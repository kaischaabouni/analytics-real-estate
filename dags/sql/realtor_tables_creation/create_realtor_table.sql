CREATE TABLE IF NOT EXISTS realtor (
    realtor_id VARCHAR NOT NULL,
    ds DATE NOT NULL,
    realtor_name VARCHAR,
    city_name VARCHAR,
    PRIMARY KEY (realtor_id, ds)
);