-- Step 1: Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS datascience_public_misc.eclipse_analytics;

-- Step 2: Create table based on the final SELECT command
CREATE OR REPLACE TABLE datascience_public_misc.eclipse_analytics.top_fees_programs (
    program_id VARCHAR,
    base_fees FLOAT,
    priority_fees FLOAT,
    units_consumed NUMBER,
    total_txs NUMBER,
    base_txs NUMBER,
    priority_txs NUMBER
);

-- Step 3: Call the procedure
CALL datascience_public_misc.eclipse_analytics.update_top_fees_programs();

-- Step 4: Define the procedure
CREATE OR REPLACE PROCEDURE datascience_public_misc.eclipse_analytics.update_top_fees_programs()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    -- Truncate the existing table
    TRUNCATE TABLE datascience_public_misc.eclipse_analytics.top_fees_programs;
    
    -- Insert fresh data
    INSERT INTO datascience_public_misc.eclipse_analytics.top_fees_programs
    WITH tx_info AS (
    SELECT 
        t.tx_id,
        t.block_timestamp,
        t.fee / pow(10, 9) as fee,
        t.units_consumed,
        (LENGTH(t.instructions::string) - LENGTH(REPLACE(t.instructions::string, 'programId', ''))) / LENGTH('programId') as total_programs,
        (SELECT 
            IFF(COUNT_IF(substr(livequery.utils.udf_base58_to_hex(e.instruction:data), 3, 2) = '03') = 1, true, false)
         FROM eclipse.core.fact_events e 
         WHERE e.tx_id = t.tx_id
         AND e.program_id = 'ComputeBudget111111111111111111111111111111'
         AND e.block_timestamp > current_date - 1
        ) as is_priority
    FROM eclipse.core.fact_transactions t
    WHERE t.block_timestamp >= CURRENT_DATE - 1
),

hyper_program_ids AS (
    SELECT instruction_program_id
    FROM eclipse.core.fact_events_inner
    WHERE program_id = 'EitxJuv2iBjsg2d7jVy2LDC1e2zBrx4GB5Y9h2Ko3A9Y'
    AND block_timestamp > '2024-08-25'
),

program_fees AS (
    SELECT 
        CASE
            WHEN program_id = 'EitxJuv2iBjsg2d7jVy2LDC1e2zBrx4GB5Y9h2Ko3A9Y' THEN 'Hyperlane Main'
            WHEN program_id = 'BgG35GxoaMgmiam3EJzcwivwQ2DTYGPTLfUCg7bhiH6V' THEN 'Hyperlane multi-sig'
            WHEN program_id = 'Hs7KVBU67nBnWhDPZkEFwWqrFMUfJbmY2DQ4gmCZfaZp' THEN 'Hyperlane igp'
            WHEN program_id IN (SELECT instruction_program_id FROM hyper_program_ids) THEN 'Hyperlane Burn'
            ELSE program_id
        END as program_id,
        e.tx_id,
        t.is_priority,
        t.fee / NULLIF(t.total_programs, 0) as attributed_fee,
        t.units_consumed / NULLIF(t.total_programs, 0) as attributed_units
    FROM eclipse.core.fact_events e
    INNER JOIN tx_info t ON e.tx_id = t.tx_id
    WHERE e.program_id NOT IN (
        'ComputeBudget111111111111111111111111111111',
        '11111111111111111111111111111111',
        'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
        'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL',
        'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb'
    )
    AND e.block_timestamp > current_date - 1
)

SELECT 
    program_id,
    SUM(CASE WHEN NOT is_priority THEN attributed_fee ELSE 0 END) as base_fees,
    SUM(CASE WHEN is_priority THEN attributed_fee ELSE 0 END) as priority_fees,
    SUM(attributed_units) as units_consumed,
    COUNT(DISTINCT tx_id) as total_txs,
    COUNT(DISTINCT CASE WHEN NOT is_priority THEN tx_id END) as base_txs,
    COUNT(DISTINCT CASE WHEN is_priority THEN tx_id END) as priority_txs
FROM program_fees
GROUP BY 1
ORDER BY (base_fees + priority_fees) DESC
LIMIT 500;

    RETURN 'Eclipse top fees programs updated successfully';
END;
$$;

-- Grant necessary permissions for INTERNAL_DEV and VELOCITY_ETHEREUM roles
GRANT USAGE ON SCHEMA datascience_public_misc.eclipse_analytics TO ROLE INTERNAL_DEV;
GRANT ALL PRIVILEGES ON TABLE datascience_public_misc.eclipse_analytics.top_fees_programs TO ROLE INTERNAL_DEV;

GRANT USAGE ON DATABASE datascience_public_misc TO ROLE VELOCITY_ETHEREUM;
GRANT USAGE ON SCHEMA datascience_public_misc.eclipse_analytics TO ROLE VELOCITY_ETHEREUM;
GRANT SELECT ON TABLE datascience_public_misc.eclipse_analytics.top_fees_programs TO ROLE VELOCITY_ETHEREUM;

-- Add clustering to improve query performance
ALTER TABLE datascience_public_misc.eclipse_analytics.top_fees_programs
CLUSTER BY (program_id);

-- Create a task to update every 12 hours
CREATE OR REPLACE TASK datascience_public_misc.eclipse_analytics.update_top_fees_programs_task
    WAREHOUSE = 'DATA_SCIENCE'
    SCHEDULE = 'USING CRON 0 */12 * * * America/Los_Angeles'
AS
    CALL datascience_public_misc.eclipse_analytics.update_top_fees_programs();

-- Resume the task
ALTER TASK datascience_public_misc.eclipse_analytics.update_top_fees_programs_task RESUME;