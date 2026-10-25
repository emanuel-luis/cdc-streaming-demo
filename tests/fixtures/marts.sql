-- The four marts as dbt builds them, with a small consistent history:
-- snapshot 3 orders (r), 2 creates (c), 2 updates (u), 1 delete (d), 1 reject, so 4 live orders.
create schema marts;

create table marts.dim_orders_current (
    order_id bigint, customer_id integer, status varchar, amount decimal(12,2),
    created_at timestamptz, updated_at timestamptz, merged_at timestamptz
);
insert into marts.dim_orders_current values
    (1, 10, 'confirmed', 12.00, '2026-10-25 10:00:00+00', '2026-10-25 11:00:00+00', '2026-10-25 12:00:00+00'),
    (2, 11, 'pending', 5.50, '2026-10-25 10:00:00+00', '2026-10-25 10:00:00+00', '2026-10-25 12:00:00+00'),
    (4, 12, 'shipped', 20.00, '2026-10-25 11:10:00+00', '2026-10-25 12:05:00+00', '2026-10-25 12:10:00+00'),
    (5, 13, 'pending', 7.25, '2026-10-25 11:20:00+00', '2026-10-25 11:20:00+00', '2026-10-25 12:10:00+00');

create table marts.fct_order_status_changes (
    event_id varchar, order_id bigint, op varchar, from_status varchar, to_status varchar,
    changed_at timestamptz, source_lsn bigint, kafka_offset bigint
);
insert into marts.fct_order_status_changes values
    ('t-0-3', 4, 'c', null, 'pending', '2026-10-25 11:10:00+00', 220, 3),
    ('t-0-4', 5, 'c', null, 'pending', '2026-10-25 11:20:00+00', 230, 4),
    ('t-0-5', 1, 'u', 'pending', 'confirmed', '2026-10-25 11:30:00+00', 240, 5),
    ('t-0-6', 4, 'u', 'pending', 'shipped', '2026-10-25 12:05:00+00', 250, 6),
    ('t-0-7', 3, 'd', 'pending', null, '2026-10-25 12:06:00+00', 260, 7);

create table marts.fct_order_activity_hourly (
    activity_hour timestamptz, op varchar, events bigint, distinct_orders bigint
);
insert into marts.fct_order_activity_hourly values
    ('2026-10-25 10:00:00+00', 'r', 3, 3),
    ('2026-10-25 11:00:00+00', 'c', 2, 2),
    ('2026-10-25 11:00:00+00', 'u', 1, 1),
    ('2026-10-25 12:00:00+00', 'u', 1, 1),
    ('2026-10-25 12:00:00+00', 'd', 1, 1);

create table marts.fct_pipeline_runs (
    component varchar, run_id varchar, batch_id bigint, started_at timestamptz,
    finished_at timestamptz, duration_seconds double, input_rows bigint, output_rows bigint,
    rejected_rows bigint
);
insert into marts.fct_pipeline_runs values
    ('ingest', 'run-a', 0, '2026-10-25 10:00:00+00', '2026-10-25 10:00:03+00', 3.0, 3, 3, 0),
    ('ingest', 'run-a', 1, '2026-10-25 12:00:00+00', '2026-10-25 12:00:04+00', 4.0, 7, 6, 1),
    ('merge', 'm1', null, '2026-10-25 12:10:00+00', '2026-10-25 12:10:05+00', 5.0, 9, 4, 0);
