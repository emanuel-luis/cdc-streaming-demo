#!/bin/sh
# Debezium's capture user: replication plus read-only access, never write access.
# Values go in as psql variables (:'x' quotes a literal, :"x" an identifier), not string pasting.
# The entrypoint may source this file when it is not executable, so no top-level exit.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
    -v cdc_user="$CDC_DB_USER" -v cdc_password="$CDC_DB_PASSWORD" -v db_name="$POSTGRES_DB" <<'SQL'
CREATE ROLE :"cdc_user" LOGIN REPLICATION PASSWORD :'cdc_password';
GRANT CONNECT ON DATABASE :"db_name" TO :"cdc_user";
GRANT USAGE ON SCHEMA public TO :"cdc_user";
GRANT SELECT ON orders TO :"cdc_user";
SQL
