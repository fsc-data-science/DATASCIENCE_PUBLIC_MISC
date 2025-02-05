select 
sweat_receiver
from datascience_public_misc.near_analytics.sweat_welcome_transfers
;


-- Step 1: Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS datascience_public_misc.near_analytics;

-- Step 2: Create table based on transaction level
CREATE OR REPLACE TABLE datascience_public_misc.near_analytics.sweat_welcome_transfers (
    block_timestamp TIMESTAMP,
    tx_hash VARCHAR,
    sweat_receiver VARCHAR,
    symbol VARCHAR,
    amount FLOAT,
    PRIMARY KEY (block_timestamp, tx_hash, sweat_receiver)
);

-- Add clustering to the table
ALTER TABLE datascience_public_misc.near_analytics.sweat_welcome_transfers
CLUSTER BY (sweat_receiver);

-- Step 3: Call the procedure
CALL datascience_public_misc.near_analytics.update_sweat_welcome_transfers();

-- Step 4: Define the procedure with a 2-day lookback period
CREATE OR REPLACE PROCEDURE datascience_public_misc.near_analytics.update_sweat_welcome_transfers()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    MERGE INTO datascience_public_misc.near_analytics.sweat_welcome_transfers AS target
    USING (
        WITH internal_addresses AS (
            SELECT address FROM (VALUES
                ('internal_sweat'),
                ('token.sweat'),
                ('sweat_oracle_0.near'),
                ('sweat_oracle_1.near'),
                ('sweat_oracle_2.near'),
                ('sweat_oracle_3.near'),
                ('sweat_oracle_4.near'),
                ('sweat_oracle_5.near'),
                ('sweat_oracle_6.near'),
                ('sweat_oracle_7.near'),
                ('sweat_oracle_8.near'),
                ('sweat_oracle_9.near'),
                ('bb273c1b4fe46a54743de83f92513d644e57423d2fee2ad4549cd4b40737f3d3'),
                ('sweat_treasury.near'),
                ('claim.sweat'),
                ('jars.sweat'),
                ('chorusone.poolv1.near'),
                ('learn.sweat'),
                ('spin.sweat'),
                ('sweat_validator.poolv1.near'),
                ('sweat_validator.near'),
                ('near'),
                ('tge-lockup.sweat')              

            ) adr(address)
        )
        SELECT 

            block_timestamp,
            tx_hash,
            to_address as sweat_receiver,
            symbol,
            amount
        FROM near.core.ez_token_transfers
        WHERE from_address = 'sweat_welcome.near'
            AND block_timestamp >= COALESCE(
                DATEADD(day, -2, (SELECT MAX(block_timestamp) FROM datascience_public_misc.near_analytics.sweat_welcome_transfers)),
                '2022-01-01'
            )
            AND transfer_type = 'native'
            AND to_address NOT IN (SELECT address FROM internal_addresses)
    ) AS source
    ON target.block_timestamp = source.block_timestamp 
        AND target.tx_hash = source.tx_hash 
        AND target.sweat_receiver = source.sweat_receiver
    WHEN MATCHED THEN
        UPDATE SET 
            target.symbol = source.symbol,
            target.amount = source.amount
    WHEN NOT MATCHED THEN
        INSERT (block_timestamp, tx_hash, sweat_receiver, symbol, amount)
        VALUES (source.block_timestamp, source.tx_hash, source.sweat_receiver, source.symbol, source.amount);

    RETURN 'SWEAT welcome transfers updated successfully';
END;
$$;

-- Create task to update sweat welcome transfers every 12 hours
CREATE OR REPLACE TASK datascience_public_misc.near_analytics.update_sweat_welcome_transfers_task
    WAREHOUSE = 'DATA_SCIENCE'
    SCHEDULE = 'USING CRON 0 */12 * * * America/Los_Angeles'
AS
    CALL datascience_public_misc.near_analytics.update_sweat_welcome_transfers();

-- Resume the task (tasks are created in suspended state by default)
ALTER TASK datascience_public_misc.near_analytics.update_sweat_welcome_transfers_task RESUME;

-- Set appropriate permissions
GRANT USAGE ON SCHEMA datascience_public_misc.near_analytics TO ROLE INTERNAL_DEV;
GRANT ALL PRIVILEGES ON TABLE datascience_public_misc.near_analytics.sweat_welcome_transfers TO ROLE INTERNAL_DEV;

-- Individual access 
GRANT USAGE ON DATABASE datascience_public_misc TO ROLE VELOCITY_ETHEREUM;
GRANT USAGE ON SCHEMA datascience_public_misc.near_analytics TO ROLE VELOCITY_ETHEREUM;
GRANT SELECT ON TABLE datascience_public_misc.near_analytics.sweat_welcome_transfers TO ROLE VELOCITY_ETHEREUM;