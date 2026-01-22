-- models/staging/stg_telegram_messages.sql
with raw as (
    select
        message_id,
        channel_name,
        message_date::timestamp as message_date,
        message_text,
        coalesce(has_media, false) as has_media,
        image_path,
        coalesce(views, 0) as views,
        coalesce(forwards, 0) as forwards,
        length(message_text) as message_length
    from {{ source('raw', 'telegram_messages') }}
    where message_id is not null
)

select * from raw
