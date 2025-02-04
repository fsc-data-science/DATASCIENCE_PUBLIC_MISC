select * from datascience_public_misc.eclipse_analytics.daily_n_signers;

-- Step 1: Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS datascience_public_misc.eclipse_analytics;

-- Step 2: Create table based on observation level (day-level)
CREATE OR REPLACE TABLE datascience_public_misc.eclipse_analytics.daily_n_signers (
    day_ DATE,
    dau INTEGER
);

-- Step 3: Call the procedure
CALL datascience_public_misc.eclipse_analytics.update_daily_n_signers();

-- Step 4: Define the procedure with a 2-day lookback period
CREATE OR REPLACE PROCEDURE datascience_public_misc.eclipse_analytics.update_daily_n_signers()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    MERGE INTO datascience_public_misc.eclipse_analytics.daily_n_signers AS target
    USING (
        SELECT 
            date_trunc('day', block_timestamp::date) as day_,
            count(distinct signers[0]) as dau
        FROM eclipse.core.fact_transactions
        WHERE day_ >= COALESCE(
            DATEADD(day, -2, (SELECT MAX(day_) FROM datascience_public_misc.eclipse_analytics.daily_n_signers)),
            '1970-01-01'
        )
        GROUP BY 1
    ) AS source
    ON target.day_ = source.day_
    WHEN MATCHED THEN
        UPDATE SET 
            target.dau = source.dau
    WHEN NOT MATCHED THEN
        INSERT (day_, dau)
        VALUES (source.day_, source.dau);

    RETURN 'Eclipse daily active signers updated successfully';
END;
$$;

-- Add clustering to optimize query performance
ALTER TABLE datascience_public_misc.eclipse_analytics.daily_n_signers
CLUSTER BY (day_);

-- Create task to update daily signers every 12 hours
CREATE OR REPLACE TASK datascience_public_misc.eclipse_analytics.update_daily_n_signers_task
    WAREHOUSE = 'DATA_SCIENCE'
    SCHEDULE = 'USING CRON 0 */12 * * * America/Los_Angeles'
AS
    CALL datascience_public_misc.eclipse_analytics.update_daily_n_signers();

-- Resume the task (tasks are created in suspended state by default)
ALTER TASK datascience_public_misc.eclipse_analytics.update_daily_n_signers_task RESUME;

-- Set appropriate permissions for INTERNAL_DEV role
GRANT USAGE ON SCHEMA datascience_public_misc.eclipse_analytics TO ROLE INTERNAL_DEV;
GRANT ALL PRIVILEGES ON TABLE datascience_public_misc.eclipse_analytics.daily_n_signers TO ROLE INTERNAL_DEV;

-- Set appropriate permissions for VELOCITY_ETHEREUM role (Studio access)
GRANT USAGE ON DATABASE datascience_public_misc TO ROLE VELOCITY_ETHEREUM;
GRANT USAGE ON SCHEMA datascience_public_misc.eclipse_analytics TO ROLE VELOCITY_ETHEREUM;
GRANT SELECT ON TABLE datascience_public_misc.eclipse_analytics.daily_n_signers TO ROLE VELOCITY_ETHEREUM;