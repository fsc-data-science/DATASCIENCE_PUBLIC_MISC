select 
signer_, first_tap_timestamp
 from datascience_public_misc.eclipse_analytics.signer_first_turbotap;

-- Step 1: Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS datascience_public_misc.eclipse_analytics;

-- Step 2: Create table based on observation level (user first tap)
CREATE OR REPLACE TABLE datascience_public_misc.eclipse_analytics.signer_first_turbotap (
    signer_ VARCHAR,
    first_tap_timestamp TIMESTAMP
);

-- Step 3: Call the procedure
CALL datascience_public_misc.eclipse_analytics.update_signer_first_turbotap();

-- Step 4: Define the procedure with a 2-day lookback period
CREATE OR REPLACE PROCEDURE datascience_public_misc.eclipse_analytics.update_signer_first_turbotap()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    MERGE INTO datascience_public_misc.eclipse_analytics.signer_first_turbotap AS target
    USING (
        SELECT 
            signers[0] as signer_,
            MIN(block_timestamp) as first_tap_timestamp
        FROM eclipse.core.fact_events
        WHERE block_timestamp::date >= COALESCE(
            DATEADD(day, -2, (SELECT MAX(first_tap_timestamp) FROM datascience_public_misc.eclipse_analytics.signer_first_turbotap)),
            '2024-12-01'
        )
        AND program_id = 'turboe9kMc3mSR8BosPkVzoHUfn5RVNzZhkrT2hdGxN'
        AND succeeded
        AND signers[0] NOT IN (
            SELECT signer_
            FROM datascience_public_misc.eclipse_analytics.signer_first_turbotap
        )
        GROUP BY signer_
    ) AS source
    ON target.signer_ = source.signer_
    WHEN NOT MATCHED THEN
        INSERT (signer_, first_tap_timestamp)
        VALUES (source.signer_, source.first_tap_timestamp);

    RETURN 'New TurboTap users updated successfully';
END;
$$;

-- Add clustering to the table for better query performance
ALTER TABLE datascience_public_misc.eclipse_analytics.signer_first_turbotap
CLUSTER BY (signer_);

-- Create task to update new tappers every 12 hours
CREATE OR REPLACE TASK datascience_public_misc.eclipse_analytics.update_signer_first_turbotap_task
    WAREHOUSE = 'DATA_SCIENCE'
    SCHEDULE = 'USING CRON 0 */12 * * * America/Los_Angeles'
AS
    CALL datascience_public_misc.eclipse_analytics.update_signer_first_turbotap();

-- Resume the task
ALTER TASK datascience_public_misc.eclipse_analytics.update_signer_first_turbotap_task RESUME;

-- Grant permissions
GRANT USAGE ON SCHEMA datascience_public_misc.eclipse_analytics TO ROLE INTERNAL_DEV;
GRANT ALL PRIVILEGES ON TABLE datascience_public_misc.eclipse_analytics.signer_first_turbotap TO ROLE INTERNAL_DEV;

GRANT USAGE ON DATABASE datascience_public_misc TO ROLE VELOCITY_ETHEREUM;
GRANT USAGE ON SCHEMA datascience_public_misc.eclipse_analytics TO ROLE VELOCITY_ETHEREUM;
GRANT SELECT ON TABLE datascience_public_misc.eclipse_analytics.signer_first_turbotap TO ROLE VELOCITY_ETHEREUM;