select * from datascience_public_misc.eclipse_analytics.daily_program_inner_tx
order by day_ desc, n_inner_tx desc
limit 10;

-- Step 1: Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS datascience_public_misc.eclipse_analytics;

-- Step 2: Create table based on program-day observation level
CREATE OR REPLACE TABLE datascience_public_misc.eclipse_analytics.daily_program_inner_tx (
    day_ TIMESTAMP,
    program_id VARCHAR,
    program_name VARCHAR,
    n_inner_tx INTEGER,
    PRIMARY KEY (day_, program_id)
);

-- Step 3: Call the procedure
CALL datascience_public_misc.eclipse_analytics.update_daily_program_inner_tx();

-- Step 4: Define the procedure with a 2-day lookback period
CREATE OR REPLACE PROCEDURE datascience_public_misc.eclipse_analytics.update_daily_program_inner_tx()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    MERGE INTO datascience_public_misc.eclipse_analytics.daily_program_inner_tx AS target
    USING (
        WITH inner_events_tx AS (
            SELECT 
                date_trunc('day', block_timestamp) as day_,
                program_id, 
                CASE
                    WHEN program_id = 'EitxJuv2iBjsg2d7jVy2LDC1e2zBrx4GB5Y9h2Ko3A9Y' THEN 'Hyperlane'
                    WHEN program_id = 'BgG35GxoaMgmiam3EJzcwivwQ2DTYGPTLfUCg7bhiH6V' THEN 'Hyperlane multi-sig'
                    WHEN program_id = 'Hs7KVBU67nBnWhDPZkEFwWqrFMUfJbmY2DQ4gmCZfaZp' THEN 'Hyperlane igp'
                    WHEN program_id = '4UsSbJQZJTfZDFrgvcPBRCSg5BbcQE6dobnriCafzj12' THEN 'Lifinity'
                    WHEN program_id = 'DcZMKcjz34CcXF1vx7CkfARZdmEja2Kcwvspu1Zw6Zmn' THEN 'SharpTrade Predictions'
                    WHEN program_id = 'iNvTyprs4TX8m6UeUEkeqDFjAL9zRCRWcexK9Sd4WEU' THEN 'Invariant Swap'
                    WHEN program_id = 'turboe9kMc3mSR8BosPkVzoHUfn5RVNzZhkrT2hdGxN' THEN 'TurboTap'
                    ELSE COALESCE(l.address_name, LEFT(program_id, 4) || '...' || RIGHT(program_id, 4))
                END AS program_name,
                COUNT(DISTINCT tx_id) AS n_inner_tx
            FROM eclipse.core.fact_events_inner i 
            LEFT JOIN eclipse.core.dim_labels l ON LOWER(i.program_id) = l.address
            WHERE day_ >= COALESCE(
                DATEADD(day, -2, (SELECT MAX(day_) FROM datascience_public_misc.eclipse_analytics.daily_program_inner_tx)),
                '1970-01-01'
            )
            AND program_id NOT IN (
                'BPFLoaderUpgradeab1e11111111111111111111111',
                'ComputeBudget111111111111111111111111111111',
                '11111111111111111111111111111111',
                'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb',
                'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL',
                'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
            )
            GROUP BY day_, program_id, address_name
        )
        SELECT * FROM inner_events_tx
    ) AS source
    ON target.day_ = source.day_ 
    AND target.program_id = source.program_id
    WHEN MATCHED THEN
        UPDATE SET 
            target.program_name = source.program_name,
            target.n_inner_tx = source.n_inner_tx
    WHEN NOT MATCHED THEN
        INSERT (day_, program_id, program_name, n_inner_tx)
        VALUES (source.day_, source.program_id, source.program_name, source.n_inner_tx);

    RETURN 'Daily program inner transactions updated successfully';
END;
$$;

-- Create task to update inner transactions every 12 hours
CREATE OR REPLACE TASK datascience_public_misc.eclipse_analytics.update_daily_program_inner_tx_task
    WAREHOUSE = 'DATA_SCIENCE'
    SCHEDULE = 'USING CRON 0 */12 * * * America/Los_Angeles'
AS
    CALL datascience_public_misc.eclipse_analytics.update_daily_program_inner_tx();

-- Resume the task (tasks are created in suspended state by default)
ALTER TASK datascience_public_misc.eclipse_analytics.update_daily_program_inner_tx_task RESUME;

-- Grant necessary permissions for Studio access
GRANT USAGE ON SCHEMA datascience_public_misc.eclipse_analytics TO ROLE INTERNAL_DEV;
GRANT ALL PRIVILEGES ON TABLE datascience_public_misc.eclipse_analytics.daily_program_inner_tx TO ROLE INTERNAL_DEV;
GRANT USAGE ON DATABASE datascience_public_misc TO ROLE VELOCITY_ETHEREUM;
GRANT USAGE ON SCHEMA datascience_public_misc.eclipse_analytics TO ROLE VELOCITY_ETHEREUM;
GRANT SELECT ON TABLE datascience_public_misc.eclipse_analytics.daily_program_inner_tx TO ROLE VELOCITY_ETHEREUM;