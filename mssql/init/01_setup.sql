-- Runs once per fresh volume, as sa, through sqlcmd (compose service mssql-init). Idempotent.
-- sqlcmd reads $(...) from the environment: SQLSERVER_DB, SQLSERVER_USER, SQLSERVER_PASSWORD,
-- SQLSERVER_CDC_USER, SQLSERVER_CDC_PASSWORD (all throwaway demo values).
SET NOCOUNT ON;
GO
IF DB_ID(N'$(SQLSERVER_DB)') IS NULL
    EXEC(N'CREATE DATABASE [$(SQLSERVER_DB)]');
GO
USE [$(SQLSERVER_DB)];
GO
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = DB_NAME() AND is_cdc_enabled = 1)
    EXEC sys.sp_cdc_enable_db;
GO
-- Same shape and constraints as postgres/initdb/01-schema.sql. No trigger: the mutator sets
-- updated_at itself.
IF OBJECT_ID(N'dbo.orders') IS NULL
    CREATE TABLE dbo.orders (
        id          BIGINT IDENTITY(1, 1) NOT NULL PRIMARY KEY,
        customer_id INT           NOT NULL,
        status      VARCHAR(20)   NOT NULL
            CONSTRAINT ck_orders_status
            CHECK (status IN ('pending', 'confirmed', 'shipped', 'delivered', 'cancelled')),
        amount      DECIMAL(12,2) NOT NULL CONSTRAINT ck_orders_amount CHECK (amount > 0),
        created_at  DATETIME2(6)  NOT NULL CONSTRAINT df_orders_created DEFAULT SYSUTCDATETIME(),
        updated_at  DATETIME2(6)  NOT NULL CONSTRAINT df_orders_updated DEFAULT SYSUTCDATETIME()
    );
GO
-- 500 seed rows by arithmetic (deterministic), before CDC is enabled on the table: they reach
-- the lake through the snapshot, as the Postgres rows reach it through Debezium's.
IF NOT EXISTS (SELECT 1 FROM dbo.orders)
   AND NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE capture_instance = N'dbo_orders')
    INSERT INTO dbo.orders (customer_id, status, amount, created_at, updated_at)
    SELECT 1 + (n * 37) % 200,
           CHOOSE(1 + n % 5, 'pending', 'confirmed', 'shipped', 'delivered', 'cancelled'),
           CAST(5 + (n * 7919 % 49500) / 100.0 AS DECIMAL(12,2)),
           DATEADD(MINUTE, -(n * 86 % 43200), SYSUTCDATETIME()),
           SYSUTCDATETIME()
    FROM (
        SELECT TOP (500) CAST(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS INT) AS n
        FROM sys.all_objects AS a CROSS JOIN sys.all_objects AS b
    ) AS numbers;
GO
-- No gating role: the reader gets SELECT on the change table directly (see the library's
-- permissions guide). Capture runs as an Agent job, so the server needs MSSQL_AGENT_ENABLED.
IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE capture_instance = N'dbo_orders')
    EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'orders',
         @capture_instance = N'dbo_orders', @role_name = NULL, @supports_net_changes = 0;
GO
USE master;
GO
IF SUSER_ID(N'$(SQLSERVER_USER)') IS NULL
    CREATE LOGIN [$(SQLSERVER_USER)] WITH PASSWORD = N'$(SQLSERVER_PASSWORD)', CHECK_POLICY = OFF;
IF SUSER_ID(N'$(SQLSERVER_CDC_USER)') IS NULL
    CREATE LOGIN [$(SQLSERVER_CDC_USER)]
        WITH PASSWORD = N'$(SQLSERVER_CDC_PASSWORD)', CHECK_POLICY = OFF;
GO
USE [$(SQLSERVER_DB)];
GO
IF USER_ID(N'$(SQLSERVER_USER)') IS NULL
    CREATE USER [$(SQLSERVER_USER)] FOR LOGIN [$(SQLSERVER_USER)];
IF USER_ID(N'$(SQLSERVER_CDC_USER)') IS NULL
    CREATE USER [$(SQLSERVER_CDC_USER)] FOR LOGIN [$(SQLSERVER_CDC_USER)];
GO
ALTER ROLE db_datareader ADD MEMBER [$(SQLSERVER_USER)];
ALTER ROLE db_datawriter ADD MEMBER [$(SQLSERVER_USER)];
GRANT SELECT ON dbo.orders TO [$(SQLSERVER_CDC_USER)];
GRANT SELECT ON cdc.[dbo_orders_CT] TO [$(SQLSERVER_CDC_USER)];
GO
SELECT capture_instance, CONVERT(varchar(22), start_lsn, 1) AS start_lsn FROM cdc.change_tables;
GO
