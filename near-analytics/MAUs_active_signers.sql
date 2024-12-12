-- Step 1: Create schema (if not already present)
CREATE SCHEMA IF NOT EXISTS datascience_public_misc.near_analytics;

-- Step 2: Create table based on month-level observations
CREATE TABLE IF NOT EXISTS datascience_public_misc.near_analytics.monthly_active_signers (
    month_ TIMESTAMP,
    n_active_signers INTEGER,
    _inserted_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    _modified_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Add clustering to optimize query performance
ALTER TABLE datascience_public_misc.near_analytics.monthly_active_signers
CLUSTER BY (month_);

-- Step 3: Call procedure
CALL datascience_public_misc.near_analytics.update_monthly_active_signers();

-- Step 4: Define procedure with 2-month lookback
CREATE OR REPLACE PROCEDURE datascience_public_misc.near_analytics.update_monthly_active_signers()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    MERGE INTO datascience_public_misc.near_analytics.monthly_active_signers AS target
    USING (
        SELECT 
            DATE_TRUNC('month', block_timestamp) as month_,
            COUNT(DISTINCT tx_signer) as n_active_signers
        FROM near.core.fact_transactions
        WHERE DATE_TRUNC('month', block_timestamp) >= COALESCE(
            DATEADD(month, -2, (SELECT MAX(month_) FROM datascience_public_misc.near_analytics.monthly_active_signers)),
            '2020-01-01' -- Setting a reasonable start date for NEAR
        )
        GROUP BY month_
    ) AS source
    ON target.month_ = source.month_
    WHEN MATCHED THEN
        UPDATE SET 
            target.n_active_signers = source.n_active_signers,
            target._modified_timestamp = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (month_, n_active_signers)
        VALUES (source.month_, source.n_active_signers);

    RETURN 'NEAR monthly active signers updated successfully';
END;
$$;

-- Create task to update monthly active signers every 12 hours
CREATE OR REPLACE TASK datascience_public_misc.near_analytics.update_monthly_active_signers_task
    WAREHOUSE = 'DATA_SCIENCE'
    SCHEDULE = 'USING CRON 0 */12 * * * America/Los_Angeles'
AS
    CALL datascience_public_misc.near_analytics.update_monthly_active_signers();

-- Resume the task (tasks are created in suspended state by default)
ALTER TASK datascience_public_misc.near_analytics.update_monthly_active_signers_task RESUME;

-- Set appropriate permissions for Studio access
-- Grant usage on database and schema
GRANT USAGE ON DATABASE datascience_public_misc TO ROLE INTERNAL_DEV;
GRANT USAGE ON SCHEMA datascience_public_misc.near_analytics TO ROLE INTERNAL_DEV;
GRANT ALL PRIVILEGES ON TABLE datascience_public_misc.near_analytics.monthly_active_signers TO ROLE INTERNAL_DEV;

-- Grant Studio access
GRANT USAGE ON DATABASE datascience_public_misc TO ROLE VELOCITY_ETHEREUM;
GRANT USAGE ON SCHEMA datascience_public_misc.near_analytics TO ROLE VELOCITY_ETHEREUM;
GRANT SELECT ON TABLE datascience_public_misc.near_analytics.monthly_active_signers TO ROLE VELOCITY_ETHEREUM;