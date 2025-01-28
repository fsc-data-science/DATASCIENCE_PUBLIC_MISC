-- Step 1: Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS datascience_public_misc.eclipse_analytics;

-- Step 2: Create table based on program-day observation level
CREATE TABLE IF NOT EXISTS datascience_public_misc.eclipse_analytics.daily_program_inner_tx (
    day_ TIMESTAMP,
    program_id VARCHAR,
    program_name VARCHAR,
    inner_tx_count INTEGER,
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
                COUNT(DISTINCT tx_id) AS inner_tx_count
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
            target.inner_tx_count = source.inner_tx_count
    WHEN NOT MATCHED THEN
        INSERT (day_, program_id, program_name, inner_tx_count)
        VALUES (source.day_, source.program_id, source.program_name, source.inner_tx_count);

    RETURN 'Daily program inner transactions updated successfully';
END;
$$;

-- Add clustering to improve query performance
ALTER TABLE datascience_public_misc.eclipse_analytics.daily_program_inner_tx
CLUSTER BY (day_);

-- Grant necessary permissions for Studio access
GRANT USAGE ON SCHEMA datascience_public_misc.eclipse_analytics TO ROLE INTERNAL_DEV;
GRANT ALL PRIVILEGES ON TABLE datascience_public_misc.eclipse_analytics.daily_program_inner_tx TO ROLE INTERNAL_DEV;
GRANT USAGE ON DATABASE datascience_public_misc TO ROLE VELOCITY_ETHEREUM;
GRANT USAGE ON SCHEMA datascience_public_misc.eclipse_analytics TO ROLE VELOCITY_ETHEREUM;
GRANT SELECT ON TABLE datascience_public_misc.eclipse_analytics.daily_program_inner_tx TO ROLE VELOCITY_ETHEREUM;