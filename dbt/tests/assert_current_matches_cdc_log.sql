-- The current table must equal the latest CDC event per order, among the events the merger has
-- consumed (offset at or below its watermark), with deleted orders left out. A difference in
-- either direction is a row.
with merged_events as (
    select e.*
    from {{ ref('stg_lake__orders_cdc_log') }} as e
    inner join {{ ref('stg_lake__merge_watermark') }} as w
        on e.kafka_topic = w.kafka_topic
        and e.kafka_partition = w.kafka_partition
    where e.kafka_offset <= w.last_offset
),

latest as (
    select *
    from merged_events
    qualify row_number() over (
        partition by order_id order by source_lsn desc, kafka_offset desc
    ) = 1
),

expected as (
    select order_id, customer_id, status, amount, created_at, updated_at
    from latest
    where op <> 'd'
),

actual as (
    select order_id, customer_id, status, amount, created_at, updated_at
    from {{ ref('stg_lake__orders_current') }}
)

select 'missing_from_current' as problem, * from (select * from expected except select * from actual)
union all
select 'unexpected_in_current' as problem, * from (select * from actual except select * from expected)
