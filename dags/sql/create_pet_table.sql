CREATE TABLE IF NOT EXISTS pet (
    ds DATE NOT NULL,
    pet_id SERIAL PRIMARY KEY,
    name VARCHAR NOT NULL,
    pet_type VARCHAR NOT NULL,
    birth_date DATE NOT NULL,
    OWNER VARCHAR NOT NULL
);