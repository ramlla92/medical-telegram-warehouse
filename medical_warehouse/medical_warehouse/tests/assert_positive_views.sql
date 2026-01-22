-- tests/assert_positive_views.sql
SELECT *
FROM {{ ref('stg_telegram_messages') }}
WHERE views < 0
