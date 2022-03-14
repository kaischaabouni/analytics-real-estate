CREATE TABLE IF NOT EXISTS event (

    event_id VARCHAR NOT NULL,
    ds DATE NOT NULL,
    event_created_date DATE ,
    event_page_main_category VARCHAR,
    realtor_id VARCHAR NOT NULL,
    PRIMARY KEY (ds, event_id)


);