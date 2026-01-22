-- tests/assert_no_future_messages.sql
SELECT *
FROM {{ ref('stg_telegram_messages') }}
WHERE message_date > CURRENT_DATE + interval '1 day'
