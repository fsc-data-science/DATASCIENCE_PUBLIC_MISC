
/*
RELIES ON datascience_public_misc.score_analytics.chain_score_distribution
*/

-- This table is accessible in flipside studio for highcharts viewing
-- most useful for seeing scores over time at chain level. 
select 
chain, -- the blockchain, e.g., aptos, arbitrum, avalanche, base.
score_date, -- the date when scores for all users on the chain were calculated.
median_score, -- The median score across all scored addresses on the chain-score_date. .e., 0, 1, 2, 3, etc. up to 15.
avg_score, -- the average score across all scored addresses on the chain as of the score_date
percentile90_score, -- 90th percentile score (uses approximation for speed which is fine) 90% of users have this score or lower. 
percentile99_score -- 99th percentile score, i.e., top 1% active users have this score or higher.
from 
datascience_public_misc.score_analytics.chain_score_spread
where chain = 'aptos' and score_date IN ('2024-10-16', '2024-09-07', '2024-08-27');
;


-- This table is accessible in flipside studio for highcharts viewing
-- most useful for seeing scores over time at chain level. 
select 
chain, -- the blockchain, e.g., aptos, arbitrum, avalanche, base.
score_date, -- the date when scores for all users on the chain were calculated.
median_score, -- The median score across all scored addresses on the chain-score_date. .e., 0, 1, 2, 3, etc. up to 15.
avg_score, -- the average score across all scored addresses on the chain as of the score_date
percentile90_score, -- 90th percentile score (uses approximation for speed which is fine) 90% of users have this score or lower. 
percentile99_score -- 99th percentile score, i.e., top 1% active users have this score or higher.
from 
datascience_public_misc.score_analytics.chain_score_spread
where chain = 'polygon' and score_date > '2024-11-07'

;

-- Step 1: Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS datascience_public_misc.score_analytics;

-- Step 2: Create table based on observation level (chain-day)
CREATE OR REPLACE TABLE datascience_public_misc.score_analytics.chain_score_spread (
    chain VARCHAR,
    score_date DATE,
    median_score FLOAT,
    avg_score FLOAT,
    percentile90_score FLOAT,
    percentile99_score FLOAT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Add clustering to improve query performance
ALTER TABLE datascience_public_misc.score_analytics.chain_score_spread
CLUSTER BY (chain, score_date);

-- Step 3: Call the procedure
CALL datascience_public_misc.score_analytics.update_chain_score_spread();

-- Step 4: Define the procedure with 2-day lookback period
CREATE OR REPLACE PROCEDURE datascience_public_misc.score_analytics.update_chain_score_spread()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    MERGE INTO datascience_public_misc.score_analytics.chain_score_spread AS target
    USING (
        WITH running_totals AS (
            SELECT 
                chain,
                score_date,
                score,
                count_with_score,
                SUM(count_with_score) OVER (PARTITION BY chain, score_date ORDER BY score) as running_sum,
                SUM(count_with_score) OVER (PARTITION BY chain, score_date) as total_count,
                count_with_score::FLOAT / SUM(count_with_score) OVER (PARTITION BY chain, score_date) as weight
            FROM datascience_public_misc.score_analytics.chain_score_distribution
            WHERE score_date >= COALESCE(
                DATEADD(day, -2, (SELECT MAX(score_date) FROM datascience_public_misc.score_analytics.chain_score_spread)),
                '2024-01-01'
            )
        ),
        
        with_shares AS (
            SELECT 
                *,
                running_sum::FLOAT / total_count as cumulative_share
            FROM running_totals
        ),
        
        daily_stats AS (
            SELECT 
                chain,
                score_date,
                -- Weighted mean (avg_score)
                SUM(score * weight) as avg_score,
                
                -- Exact median from cumulative distribution
                MIN(CASE WHEN cumulative_share >= 0.50 THEN score END) as median_score,
                
                -- P90 (90th percentile)
                MIN(CASE WHEN cumulative_share >= 0.90 THEN score END) as percentile90_score,
                
                -- P99 (99th percentile)
                MIN(CASE WHEN cumulative_share >= 0.99 THEN score END) as percentile99_score
                
            FROM with_shares
            GROUP BY chain, score_date
        )
        
        SELECT * FROM daily_stats
    ) AS source
    ON target.chain = source.chain 
    AND target.score_date = source.score_date
    WHEN MATCHED THEN
        UPDATE SET 
            median_score = source.median_score,
            avg_score = source.avg_score,
            percentile90_score = source.percentile90_score,
            percentile99_score = source.percentile99_score,
            updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (
            chain,
            score_date,
            median_score,
            avg_score,
            percentile90_score,
            percentile99_score,
            updated_at
        )
        VALUES (
            source.chain,
            source.score_date,
            source.median_score,
            source.avg_score,
            source.percentile90_score,
            source.percentile99_score,
            CURRENT_TIMESTAMP()
        );

    RETURN 'Chain score spread metrics updated successfully using score distribution data';
END;
$$;

-- Create task to update chain score spread every 12 hours
CREATE OR REPLACE TASK datascience_public_misc.score_analytics.update_chain_score_spread_task
    WAREHOUSE = 'DATA_SCIENCE'
    SCHEDULE = 'USING CRON 0 */12 * * * America/Los_Angeles'
AS
    CALL datascience_public_misc.score_analytics.update_chain_score_spread();

-- Resume the task
ALTER TASK datascience_public_misc.score_analytics.update_chain_score_spread_task RESUME;

-- Set appropriate permissions
GRANT USAGE ON SCHEMA datascience_public_misc.score_analytics TO ROLE INTERNAL_DEV;
GRANT ALL PRIVILEGES ON TABLE datascience_public_misc.score_analytics.chain_score_spread TO ROLE INTERNAL_DEV;

-- Individual access for Velocity Ethereum
GRANT USAGE ON DATABASE datascience_public_misc TO ROLE VELOCITY_ETHEREUM;
GRANT USAGE ON SCHEMA datascience_public_misc.score_analytics TO ROLE VELOCITY_ETHEREUM;
GRANT SELECT ON TABLE datascience_public_misc.score_analytics.chain_score_spread TO ROLE VELOCITY_ETHEREUM;