CREATE TABLE
    agents_per_realtor
AS SELECT
    {{ params.table_realtor }}.realtor_id,
    {{ params.table_realtor }}.realtor_name,
    {{ params.table_realtor }}.city_name,
    {{ params.table_agent }}.ds,
    {{ params.table_agent }}.is_enabled,
    {{ params.table_agent }}.role, 
    COUNT({{ params.table_agent }}.realtor_agent_id) AS number_of_agents
FROM
    {{ params.table_agent }} INNER JOIN {{ params.table_realtor }}
ON
    {{ params.table_agent }}.realtor_id = {{ params.table_realtor }}.realtor_id
    AND {{ params.table_agent }}.ds = {{ params.table_realtor }}.ds
GROUP BY
    {{ params.table_realtor }}.realtor_id,
    {{ params.table_realtor }}.realtor_name,
    {{ params.table_realtor }}.city_name,
    {{ params.table_agent }}.ds,
    {{ params.table_agent }}.is_enabled,
    {{ params.table_agent }}.role

