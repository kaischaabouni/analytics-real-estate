CREATE TABLE
    reviews_per_realtor
AS SELECT
    {{ params.table_realtor }}.realtor_id,
    {{ params.table_realtor }}.realtor_name,
    {{ params.table_realtor }}.city_name,
    {{ params.table_review }}.ds,
    {{ params.table_review }}.type_avis,
    {{ params.table_review }}.moderation_status,
    TO_CHAR(
        {{ params.table_review }}.review_ts,
        'YYYY-MM-DD'
    ) as review_date,

    COUNT({{ params.table_review }}.review_id) AS number_of_reviews,
    SUM({{ params.table_review }}.realtor_recommendation) AS sum_review_notes
FROM
    {{ params.table_review }} INNER JOIN {{ params.table_realtor }}
ON
    {{ params.table_review }}.realtor_id = {{ params.table_realtor }}.realtor_id
    AND {{ params.table_review }}.ds = {{ params.table_realtor }}.ds
GROUP BY
    {{ params.table_realtor }}.realtor_id,
    {{ params.table_realtor }}.realtor_name,
    {{ params.table_realtor }}.city_name,
    {{ params.table_review }}.ds,
    {{ params.table_review }}.type_avis,
    {{ params.table_review }}.moderation_status,
    review_date

