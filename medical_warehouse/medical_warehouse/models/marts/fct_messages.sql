-- models/marts/fct_messages.sql
select
    s.message_id,
    c.channel_key,
    d.date_key,
    s.message_text,
    s.message_length,
    s.views as view_count,
    s.forwards as forward_count,
    s.has_media as has_image,
    s.image_path
from {{ ref('stg_telegram_messages') }} s
join {{ ref('dim_channels') }} c
  on s.channel_name = c.channel_name
join {{ ref('dim_dates') }} d
  on s.message_date::date = d.full_date
