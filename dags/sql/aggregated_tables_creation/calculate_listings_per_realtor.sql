CREATE TABLE
    listings_per_realtor
AS SELECT
    {{ params.table_realtor }}.realtor_id,
    {{ params.table_realtor }}.realtor_name,
    {{ params.table_realtor }}.city_name,
    {{ params.table_listing }}.ds,
    TO_CHAR(
        {{ params.table_listing }}.start_ts,
        'YYYY-MM-DD'
    ) as publication_date,
    {{ params.table_listing }}.transaction_type,
    {{ params.table_listing }}.item_type,
    COUNT({{ params.table_listing }}.listing_id) AS number_of_listings
FROM
    {{ params.table_listing }} INNER JOIN {{ params.table_realtor }}
ON
    {{ params.table_listing }}.realtor_id = {{ params.table_realtor }}.realtor_id
    AND {{ params.table_listing }}.ds = {{ params.table_realtor }}.ds
GROUP BY
    {{ params.table_realtor }}.realtor_id,
    {{ params.table_realtor }}.realtor_name,
    {{ params.table_realtor }}.city_name,
    {{ params.table_listing }}.ds,
    publication_date,
    {{ params.table_listing }}.transaction_type,
    {{ params.table_listing }}.item_type

