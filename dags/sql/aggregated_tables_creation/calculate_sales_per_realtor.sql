CREATE TABLE
    sales_per_realtor
AS SELECT
    {{ params.table_realtor }}.realtor_id,
    {{ params.table_realtor }}.realtor_name,
    {{ params.table_realtor }}.city_name,
    
    TO_CHAR(
        {{ params.table_past_sale }}.sale_ts,
        'YYYY-MM-DD'
    ) as sale_date,
    {{ params.table_past_sale }}.item_type,
    {{ params.table_past_sale }}.ds,
    COUNT({{ params.table_past_sale }}.past_sale_id) AS number_of_sales
FROM
    {{ params.table_past_sale }} INNER JOIN {{ params.table_realtor }}
ON
    {{ params.table_past_sale }}.realtor_id = {{ params.table_realtor }}.realtor_id
    AND {{ params.table_past_sale }}.ds = {{ params.table_realtor }}.ds
GROUP BY
    {{ params.table_realtor }}.realtor_id,
    {{ params.table_realtor }}.realtor_name,
    {{ params.table_realtor }}.city_name,
    sale_date,
    {{ params.table_past_sale }}.item_type,
    {{ params.table_past_sale }}.ds

