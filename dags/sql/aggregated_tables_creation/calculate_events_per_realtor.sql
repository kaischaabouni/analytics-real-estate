CREATE TABLE
    events_per_realtor
AS SELECT
    {{ params.table_realtor }}.realtor_id,
    {{ params.table_realtor }}.realtor_name,
    {{ params.table_realtor }}.city_name,
    {{ params.table_event }}.ds,
    TO_CHAR(
        {{ params.table_event }}.event_created_date,
        'YYYY-MM-DD'
    ) as create_date,
    {{ params.table_event }}.event_page_main_category,
    COUNT({{ params.table_event }}.event_id) AS number_of_events
FROM
    {{ params.table_event }} INNER JOIN {{ params.table_realtor }}
ON
    {{ params.table_event }}.realtor_id = {{ params.table_realtor }}.realtor_id
    AND {{ params.table_event }}.ds = {{ params.table_realtor }}.ds
GROUP BY
    {{ params.table_realtor }}.realtor_id,
    {{ params.table_realtor }}.realtor_name,
    {{ params.table_realtor }}.city_name,
    {{ params.table_event }}.ds,
    create_date,
    {{ params.table_event }}.event_page_main_category

