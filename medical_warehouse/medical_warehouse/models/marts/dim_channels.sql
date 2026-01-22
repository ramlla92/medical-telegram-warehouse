-- models/marts/dim_channels.sql

with channel_data as (
    select
        distinct s.channel_name,
        case
            when lower(s.channel_name) like '%chemed%' then 'Medical'
            when lower(s.channel_name) like '%tikvah%' then 'Pharmaceutical'
            when lower(s.channel_name) like '%lobelia%' then 'Cosmetics'
            else 'Other'
        end as channel_type
    from {{ ref('stg_telegram_messages') }} s
),

aggregated as (
    select
        row_number() over () as channel_key,
        c.channel_name,
        c.channel_type,
        min(s.message_date) as first_post_date,
        max(s.message_date) as last_post_date,
        count(*) as total_posts,
        avg(s.views) as avg_views
    from channel_data c
    join {{ ref('stg_telegram_messages') }} s
      on c.channel_name = s.channel_name
    group by c.channel_name, c.channel_type
)

select * from aggregated
