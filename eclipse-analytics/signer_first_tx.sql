select 
    signer_,
    first_tx_date
from datascience_public_misc.eclipse_analytics.signer_first_timestamp;


-- Step 1: Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS datascience_public_misc.eclipse_analytics;

-- Step 2: Create table based on signer-level observation
CREATE OR REPLACE TABLE datascience_public_misc.eclipse_analytics.signer_first_timestamp (
    signer_ VARCHAR,
    first_tx_date TIMESTAMP
);

-- Step 3: Call the procedure
CALL datascience_public_misc.eclipse_analytics.update_signer_first_timestamp();

-- Step 4: Define the procedure with a 2-day lookback period
CREATE OR REPLACE PROCEDURE datascience_public_misc.eclipse_analytics.update_signer_first_timestamp()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    MERGE INTO datascience_public_misc.eclipse_analytics.signer_first_timestamp AS target
    USING (
        WITH potential_new_signers AS (
            SELECT 
                signers[0] as signer_,
                MIN(block_timestamp) as first_tx_date
            FROM eclipse.core.fact_transactions
            WHERE succeeded = TRUE
            AND block_timestamp >= COALESCE(
                DATEADD(day, -2, (SELECT MAX(first_tx_date) FROM datascience_public_misc.eclipse_analytics.signer_first_timestamp)),
                '1970-01-01'
            )
            GROUP BY signer_
        )
        SELECT p.signer_, p.first_tx_date
        FROM potential_new_signers p
        LEFT JOIN datascience_public_misc.eclipse_analytics.signer_first_timestamp e
            ON p.signer_ = e.signer_
        WHERE e.signer_ IS NULL
        AND p.signer_ IS NOT NULL
    ) AS source
    ON target.signer_ = source.signer_
    WHEN NOT MATCHED THEN
        INSERT (signer_, first_tx_date)
        VALUES (source.signer_, source.first_tx_date);

    RETURN 'Eclipse signer first timestamps updated successfully';
END;
$$;

-- Add clustering to improve query performance
ALTER TABLE datascience_public_misc.eclipse_analytics.signer_first_timestamp
CLUSTER BY (signer_);

CREATE OR REPLACE TASK datascience_public_misc.eclipse_analytics.update_signer_first_timestamp_task
    WAREHOUSE = 'DATA_SCIENCE'
    SCHEDULE = 'USING CRON 0 0 * * * America/Los_Angeles'
AS
    CALL datascience_public_misc.eclipse_analytics.update_signer_first_timestamp();


-- Resume the task
ALTER TASK datascience_public_misc.eclipse_analytics.update_signer_first_timestamp_task RESUME;


-- Grant schema usage
GRANT USAGE ON SCHEMA datascience_public_misc.eclipse_analytics TO ROLE INTERNAL_DEV;
GRANT ALL PRIVILEGES ON TABLE datascience_public_misc.eclipse_analytics.signer_first_timestamp TO ROLE INTERNAL_DEV;


-- Individual access for Velocity Ethereum
GRANT USAGE ON DATABASE datascience_public_misc TO ROLE VELOCITY_ETHEREUM;
GRANT USAGE ON SCHEMA datascience_public_misc.eclipse_analytics TO ROLE VELOCITY_ETHEREUM;
GRANT SELECT ON TABLE datascience_public_misc.eclipse_analytics.signer_first_timestamp TO ROLE VELOCITY_ETHEREUM;