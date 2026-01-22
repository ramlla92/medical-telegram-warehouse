-- models/marts/fct_image_detections.sql

with yolo as (
    select *
    from raw.yolo_detections
),
image_class as (
    select *
    from raw.image_classification
)

select
    y.message_id,
    f.channel_key,
    f.date_key,
    y.detected_class,
    y.confidence_score,
    ic.image_category
from yolo y
join {{ ref('fct_messages') }} f   -- use ref here instead of hardcoding schema
    on y.message_id = f.message_id
left join image_class ic
    on y.message_id = ic.message_id
