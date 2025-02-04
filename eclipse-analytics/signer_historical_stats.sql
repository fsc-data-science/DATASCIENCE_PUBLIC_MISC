-- Step 1: Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS datascience_public_misc.eclipse_analytics;

-- Step 2: Create table based on observation level
CREATE OR REPLACE TABLE datascience_public_misc.eclipse_analytics.signer_historical_stats (
    avg_txs FLOAT,
    median_txs FLOAT,
    avg_txs_per_active_day FLOAT,
    median_txs_per_active_day FLOAT,
    avg_programs_used FLOAT,
    avg_days_active FLOAT,
    avg_days_since_first_tx FLOAT
);

-- Step 3: Call procedure
CALL datascience_public_misc.eclipse_analytics.update_signer_historical_stats();

-- Step 4: Define procedure
CREATE OR REPLACE PROCEDURE datascience_public_misc.eclipse_analytics.update_signer_historical_stats()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    -- Truncate existing data
    TRUNCATE TABLE datascience_public_misc.eclipse_analytics.signer_historical_stats;
    
    -- Insert new data
    INSERT INTO datascience_public_misc.eclipse_analytics.signer_historical_stats (
        avg_txs,
        median_txs,
        avg_txs_per_active_day,
        median_txs_per_active_day,
        avg_programs_used,
        avg_days_active,
        avg_days_since_first_tx
    )
    WITH signer_tx_history AS (
        SELECT 
            signers[0] as signer,
            count(tx_id) as txs,
            count(distinct date_trunc('day', block_timestamp)) as days_active,
            DIV0(txs, days_active) as txs_per_active_day,
            min(block_timestamp) as first_tx_date,
            max(block_timestamp) as last_tx_date,
            datediff('day', first_tx_date, current_date) as days_since_first_tx
        FROM eclipse.core.fact_transactions
        WHERE signers[0] != 'G5FM3UKwcBJ47PwLWLLY1RQpqNtTMgnqnd6nZGcJqaBp'
        GROUP BY signer
    ),
    signer_program_history AS (
        SELECT 
            signers[0] as signer,
            count(distinct program_id) as programs_used
        FROM eclipse.core.fact_events
        WHERE program_id NOT IN (
            'BPFLoaderUpgradeab1e11111111111111111111111',
            'ComputeBudget111111111111111111111111111111',
            '11111111111111111111111111111111',
            'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb',
            'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL',
            'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
            'FUCHhfHbuxXBWiRBfVdhmiog84sUJw11aAq3ibAUGL6e',
            '5hEa5j38yNJRM9vQA44Q6gXVj4Db8y3mWxkDtQeofKKs'
        )
        AND signers[0] != 'G5FM3UKwcBJ47PwLWLLY1RQpqNtTMgnqnd6nZGcJqaBp'
        GROUP BY signer
    )
    SELECT 
        AVG(txs) as avg_txs,
        median(txs) as median_txs,
        AVG(txs_per_active_day) as avg_txs_per_active_day,
        median(txs_per_active_day) as median_txs_per_active_day,
        (SELECT AVG(programs_used) FROM signer_program_history) as avg_programs_used,
        AVG(days_active) as avg_days_active,
        AVG(days_since_first_tx) as avg_days_since_first_tx
    FROM signer_tx_history;
    
    RETURN 'Signer historical stats updated successfully';
END;
$$;

-- Create task to update stats every 24 hours
CREATE OR REPLACE TASK datascience_public_misc.eclipse_analytics.update_signer_stats_task
    WAREHOUSE = 'DATA_SCIENCE'
    SCHEDULE = 'USING CRON 0 0 * * * America/Los_Angeles'
AS
    CALL datascience_public_misc.eclipse_analytics.update_signer_historical_stats();

-- Resume the task
ALTER TASK datascience_public_misc.eclipse_analytics.update_signer_stats_task RESUME;


-- Grant schema usage
GRANT USAGE ON SCHEMA datascience_public_misc.eclipse_analytics TO ROLE INTERNAL_DEV;
GRANT ALL PRIVILEGES ON TABLE datascience_public_misc.eclipse_analytics.signer_historical_stats TO ROLE INTERNAL_DEV;

-- Individual access for Velocity Ethereum
GRANT USAGE ON DATABASE datascience_public_misc TO ROLE VELOCITY_ETHEREUM;
GRANT USAGE ON SCHEMA datascience_public_misc.eclipse_analytics TO ROLE VELOCITY_ETHEREUM;
GRANT SELECT ON TABLE datascience_public_misc.eclipse_analytics.signer_historical_stats TO ROLE VELOCITY_ETHEREUM;