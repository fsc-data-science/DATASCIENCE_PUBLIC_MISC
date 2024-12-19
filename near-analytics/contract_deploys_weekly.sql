select * from datascience_public_misc.near_analytics.weekly_contract_metrics;

-- Step 1: Create schema
CREATE SCHEMA IF NOT EXISTS datascience_public_misc.near_analytics;

-- Step 2: Create table based on week-predecessor level
CREATE TABLE IF NOT EXISTS datascience_public_misc.near_analytics.weekly_contract_metrics (
    week_ TIMESTAMP,
    predecessor_id VARCHAR,
    num_deploys INTEGER,
    num_unique_signers INTEGER,
    num_unique_receivers INTEGER
);

-- Step 3: Call the procedure
CALL datascience_public_misc.near_analytics.update_weekly_contract_metrics();

-- Step 4: Define procedure with 4-week lookback
CREATE OR REPLACE PROCEDURE datascience_public_misc.near_analytics.update_weekly_contract_metrics()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    MERGE INTO datascience_public_misc.near_analytics.weekly_contract_metrics AS target
    USING (
        SELECT 
            date_trunc('week', block_timestamp) as week_,
            predecessor_id,
            count(distinct action_id) as num_deploys,
            count(distinct signer_id) as num_unique_signers,
            count(distinct receiver_id) as num_unique_receivers
        FROM near.core.fact_actions_events
        WHERE ACTION_NAME = 'DeployContract'
        AND receipt_succeeded = true
        AND date_trunc('week', block_timestamp) >= COALESCE(
            DATEADD(week, -4, (SELECT MAX(week_) FROM datascience_public_misc.near_analytics.weekly_contract_metrics)),
            '1970-01-01'
        )
        GROUP BY 1, 2
    ) AS source
    ON target.week_ = source.week_ 
    AND target.predecessor_id = source.predecessor_id
    WHEN MATCHED THEN
        UPDATE SET 
            num_deploys = source.num_deploys,
            num_unique_signers = source.num_unique_signers,
            num_unique_receivers = source.num_unique_receivers
    WHEN NOT MATCHED THEN
        INSERT (week_, predecessor_id, num_deploys, num_unique_signers, num_unique_receivers)
        VALUES (source.week_, source.predecessor_id, source.num_deploys, source.num_unique_signers, source.num_unique_receivers);

    RETURN 'Weekly contract metrics updated successfully';
END;
$$;

-- Add clustering
ALTER TABLE datascience_public_misc.near_analytics.weekly_contract_metrics
CLUSTER BY (week_, predecessor_id);

-- Set appropriate permissions
GRANT USAGE ON SCHEMA datascience_public_misc.near_analytics TO ROLE INTERNAL_DEV;
GRANT ALL PRIVILEGES ON TABLE datascience_public_misc.near_analytics.weekly_contract_metrics TO ROLE INTERNAL_DEV;

-- Individual access 
GRANT USAGE ON DATABASE datascience_public_misc TO ROLE VELOCITY_ETHEREUM;
GRANT USAGE ON SCHEMA datascience_public_misc.near_analytics TO ROLE VELOCITY_ETHEREUM;
GRANT SELECT ON TABLE datascience_public_misc.near_analytics.weekly_contract_metrics TO ROLE VELOCITY_ETHEREUM;