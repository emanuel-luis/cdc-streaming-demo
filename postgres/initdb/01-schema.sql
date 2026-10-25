-- Runs once, as the database owner, on a fresh data directory.
CREATE TABLE orders (
    id          BIGSERIAL PRIMARY KEY,
    customer_id INT           NOT NULL,
    status      VARCHAR(20)   NOT NULL
        CHECK (status IN ('pending', 'confirmed', 'shipped', 'delivered', 'cancelled')),
    amount      NUMERIC(12,2) NOT NULL CHECK (amount > 0),
    created_at  TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE FUNCTION orders_touch_updated_at() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$;

CREATE TRIGGER orders_set_updated_at
    BEFORE UPDATE ON orders
    FOR EACH ROW EXECUTE FUNCTION orders_touch_updated_at();

-- Updates and deletes must carry the full before image (status transitions).
ALTER TABLE orders REPLICA IDENTITY FULL;

-- Same seed, same session: every fresh stack starts from the same 500 rows.
SELECT setseed(0.42);

INSERT INTO orders (customer_id, status, amount, created_at)
SELECT 1 + floor(random() * 200)::int,
       (ARRAY['pending', 'confirmed', 'shipped', 'delivered', 'cancelled'])[1 + floor(random() * 5)::int],
       round((5 + random() * 495)::numeric, 2),
       now() - random() * interval '30 days'
FROM generate_series(1, 500);

CREATE PUBLICATION cdc_demo_pub FOR TABLE orders;
