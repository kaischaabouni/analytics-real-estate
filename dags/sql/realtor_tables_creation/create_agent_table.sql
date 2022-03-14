CREATE TABLE IF NOT EXISTS agent (
    ds DATE NOT NULL,
    realtor_agent_id VARCHAR NOT NULL,
    realtor_agent_name VARCHAR,
    realtor_id VARCHAR NOT NULL,
    user_id VARCHAR NOT NULL,
    user_name VARCHAR,
    is_enabled BOOLEAN,
    role VARCHAR,
    role_label VARCHAR,
    PRIMARY KEY (realtor_agent_id,ds)

);