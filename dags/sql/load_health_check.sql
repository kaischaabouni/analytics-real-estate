INSERT INTO health_check (
    datestamp,
    status
)
VALUES (
    '{{ ds }}',
    'OK'
);
