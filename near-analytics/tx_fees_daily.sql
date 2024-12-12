
/* 
NEAR Daily Transaction Fees --------------------------------------------------------------------------------
*/

SELECT *
FROM datascience_public_misc.near_analytics.near_daily_fees
LIMIT 10;

-- Step 2: Create table based on observation level
CREATE TABLE IF NOT EXISTS datascience_public_misc.near_analytics.near_daily_fees (
    day_ DATE,
    fees FLOAT
);

-- Step 3: Call the procedure
CALL datascience_public_misc.near_analytics.update_near_daily_fees();

-- Step 4: Define the procedure with an intelligent lookback period
CREATE OR REPLACE PROCEDURE datascience_public_misc.near_analytics.update_near_daily_fees()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
MERGE INTO datascience_public_misc.near_analytics.near_daily_fees AS target
USING (
    SELECT
        DATE_TRUNC('day', BLOCK_TIMESTAMP) AS day_,
        SUM(DIV0(TRANSACTION_FEE, 1e24)) AS fees
    FROM near.core.fact_transactions
    WHERE BLOCK_TIMESTAMP >= COALESCE(
        DATEADD(day, -2, (SELECT MAX(day_) FROM datascience_public_misc.near_analytics.near_daily_fees)),
        '1970-01-01'
    )
    GROUP BY 1
) AS source
ON target.day_ = source.day_
WHEN MATCHED THEN
    UPDATE SET
        target.fees = source.fees
WHEN NOT MATCHED THEN
    INSERT (day_, fees)
    VALUES (source.day_, source.fees);

RETURN 'NEAR daily fees updated successfully';
END;
$$;

-- Step 5: Add clustering to the table
ALTER TABLE datascience_public_misc.near_analytics.near_daily_fees
CLUSTER BY (day_);


-- Create task to update data every 12 hours
CREATE OR REPLACE TASK datascience_public_misc.near_analytics.update_near_daily_fees_task
    WAREHOUSE = 'DATA_SCIENCE'
    SCHEDULE = 'USING CRON 0 */12 * * * America/Los_Angeles'
AS
    CALL datascience_public_misc.near_analytics.update_near_daily_fees();

-- Resume the task
ALTER TASK datascience_public_misc.near_analytics.update_near_daily_fees_task RESUME;

-- Set appropriate permissions for Studio access
-- Grant usage on database and schema
GRANT USAGE ON DATABASE datascience_public_misc TO ROLE INTERNAL_DEV;
GRANT USAGE ON SCHEMA datascience_public_misc.near_analytics TO ROLE INTERNAL_DEV;
GRANT ALL PRIVILEGES ON TABLE datascience_public_misc.near_analytics.near_daily_fees TO ROLE INTERNAL_DEV;

-- Grant Studio access
GRANT USAGE ON DATABASE datascience_public_misc TO ROLE VELOCITY_ETHEREUM;
GRANT USAGE ON SCHEMA datascience_public_misc.near_analytics TO ROLE VELOCITY_ETHEREUM;
GRANT SELECT ON TABLE datascience_public_misc.near_analytics.near_daily_fees TO ROLE VELOCITY_ETHEREUM;