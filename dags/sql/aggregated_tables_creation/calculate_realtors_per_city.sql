CREATE TABLE
    realtors_per_city
AS SELECT
    {{ params.table_realtor }}.ds,
    {{ params.table_realtor }}.city_name,
    COUNT({{ params.table_realtor }}.realtor_id) AS number_of_realtors
FROM
    {{ params.table_realtor }} 
GROUP BY
    {{ params.table_realtor }}.ds,
    {{ params.table_realtor }}.city_name

