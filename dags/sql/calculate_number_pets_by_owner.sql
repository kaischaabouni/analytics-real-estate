CREATE TABLE
    owner_stats
AS SELECT
    owner,
    COUNT(*) AS number_of_pets
FROM
    {{ params.table_name }}
GROUP BY
    1
ORDER BY
    1 DESC
