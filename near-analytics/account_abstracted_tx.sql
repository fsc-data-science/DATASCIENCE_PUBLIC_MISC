-- Step 1: Create schema (if not already present)
CREATE SCHEMA IF NOT EXISTS datascience_public_misc.near_analytics;

-- Step 2: Create table based on daily-level observations
CREATE OR REPLACE TABLE datascience_public_misc.near_analytics.account_abstraction_tx (
    day_ TIMESTAMP,
    wallet_ VARCHAR,
    n_abstracted_tx INTEGER,
    _inserted_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    _modified_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Add clustering to optimize query performance
ALTER TABLE datascience_public_misc.near_analytics.account_abstraction_tx
CLUSTER BY (day_);

-- Step 3: Call procedure
CALL datascience_public_misc.near_analytics.update_account_abstraction_tx();

-- Step 4: Define procedure with 2-day lookback
CREATE OR REPLACE PROCEDURE datascience_public_misc.near_analytics.update_account_abstraction_tx()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    MERGE INTO datascience_public_misc.near_analytics.account_abstraction_tx AS target
    USING (
        WITH wallet_target AS (
            SELECT tx_signer_address
            FROM (
                VALUES 
                    ('relay.tg'),
                    ('hotwallet.kaiching'),
                    ('0-relay.hot.tg'),
                    ('users.kaiching'),
                    ('sweat-relayer.near')
            ) AS v(tx_signer_address)
        )
        SELECT 
            DATE_TRUNC('day', t.block_timestamp) as day_,
            CASE 
                WHEN t.tx_signer ILIKE '%tg' THEN 'TG'
                WHEN t.tx_signer ILIKE '%kaiching' THEN 'Kaiching'
                WHEN t.tx_signer ILIKE 'sweat%' THEN 'Sweat'
            END as wallet_,
            COUNT(t.tx_hash) as n_abstracted_tx
        FROM near.core.fact_transactions t
        INNER JOIN wallet_target w
            ON t.tx_signer = w.tx_signer_address
        WHERE DATE_TRUNC('day', t.block_timestamp) >= COALESCE(
            DATEADD(day, -2, (SELECT MAX(day_) FROM datascience_public_misc.near_analytics.account_abstraction_tx)),
            '2024-01-01' -- Setting start date as per original query
        )
        GROUP BY day_, wallet_
    ) AS source
    ON target.day_ = source.day_ 
    AND target.wallet_ = source.wallet_
    WHEN MATCHED THEN
        UPDATE SET 
            target.n_abstracted_tx = source.n_abstracted_tx,
            target._modified_timestamp = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (day_, wallet_, n_abstracted_tx)
        VALUES (source.day_, source.wallet_, source.n_abstracted_tx);

    RETURN 'NEAR account abstraction transactions updated successfully';
END;
$$;

-- Create task to update data every 12 hours
CREATE OR REPLACE TASK datascience_public_misc.near_analytics.update_account_abstraction_tx_task
    WAREHOUSE = 'DATA_SCIENCE'
    SCHEDULE = 'USING CRON 0 */12 * * * America/Los_Angeles'
AS
    CALL datascience_public_misc.near_analytics.update_account_abstraction_tx();

-- Resume the task
ALTER TASK datascience_public_misc.near_analytics.update_account_abstraction_tx_task RESUME;

-- Set appropriate permissions for Studio access
-- Grant usage on database and schema
GRANT USAGE ON DATABASE datascience_public_misc TO ROLE INTERNAL_DEV;
GRANT USAGE ON SCHEMA datascience_public_misc.near_analytics TO ROLE INTERNAL_DEV;
GRANT ALL PRIVILEGES ON TABLE datascience_public_misc.near_analytics.account_abstraction_tx TO ROLE INTERNAL_DEV;

-- Grant Studio access
GRANT USAGE ON DATABASE datascience_public_misc TO ROLE VELOCITY_ETHEREUM;
GRANT USAGE ON SCHEMA datascience_public_misc.near_analytics TO ROLE VELOCITY_ETHEREUM;
GRANT SELECT ON TABLE datascience_public_misc.near_analytics.account_abstraction_tx TO ROLE VELOCITY_ETHEREUM;