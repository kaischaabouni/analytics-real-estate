CREATE TABLE IF NOT EXISTS review (
    review_id VARCHAR NOT NULL,
    ds DATE NOT NULL,
    review_name  VARCHAR,
    review_ts TIMESTAMP,
    realtor_id VARCHAR NOT NULL,
    type_avis VARCHAR,
    moderation_status VARCHAR,
    realtor_recommendation smallint,
    PRIMARY KEY (ds, review_id)
);