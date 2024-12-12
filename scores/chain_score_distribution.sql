select 
chain, -- the blockchain, e.g., aptos, arbitrum, avalanche, base.
score_date, -- the date when scores for all users on the chain were calculated.
score, -- The score itself, i.e., 0, 1, 2, 3, etc. up to 15.
count_with_score -- The # of addresses with that score. 
from datascience_public_misc.score_analytics.chain_score_distribution
where score_date = '2024-11-20'
and chain = 'sei'
;

select 
* from
datascience_public_misc.score_analytics.chain_score_distribution
where chain = 'polygon'
and date_trunc('week', score_date) = '2024-12-02' 
order by score_date asc, score asc
;

-- Step 1: Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS datascience_public_misc.score_analytics;

-- Step 2: Create table based on observation level (chain-day-score)
CREATE OR REPLACE TABLE datascience_public_misc.score_analytics.chain_score_distribution (
    chain VARCHAR,
    score_date DATE,
    score INTEGER,
    count_with_score INTEGER,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Step 3: Call the procedure
CALL datascience_public_misc.score_analytics.update_chain_score_distribution();

-- Step 4: Define the procedure with 2-day lookback period
CREATE OR REPLACE PROCEDURE datascience_public_misc.score_analytics.update_chain_score_distribution()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    MERGE INTO datascience_public_misc.score_analytics.chain_score_distribution AS target
    USING (
        WITH base_scores AS (
            SELECT 
                'aptos' as chain,
                score_date,
                total_score as score,
                COUNT(*) as count_with_score
            FROM datascience.onchain_scores.aptos
            WHERE score_date >= COALESCE(
                DATEADD(day, -2, (SELECT MAX(score_date) FROM datascience_public_misc.score_analytics.chain_score_distribution)),
                '2024-01-01'
            )
            GROUP BY chain, score_date, total_score

            UNION ALL

            SELECT 
                'arbitrum' as chain,
                score_date,
                total_score as score,
                COUNT(*) as count_with_score
            FROM datascience.onchain_scores.arbitrum
            WHERE score_date >= COALESCE(
                DATEADD(day, -2, (SELECT MAX(score_date) FROM datascience_public_misc.score_analytics.chain_score_distribution)),
                '2024-01-01'
            )
            GROUP BY chain, score_date, total_score

            UNION ALL

            SELECT 
                'avalanche' as chain,
                score_date,
                total_score as score,
                COUNT(*) as count_with_score
            FROM datascience.onchain_scores.avalanche
            WHERE score_date >= COALESCE(
                DATEADD(day, -2, (SELECT MAX(score_date) FROM datascience_public_misc.score_analytics.chain_score_distribution)),
                '2024-01-01'
            )
            GROUP BY chain, score_date, total_score

            UNION ALL

            SELECT 
                'axelar' as chain,
                score_date,
                total_score as score,
                COUNT(*) as count_with_score
            FROM datascience.onchain_scores.axelar
            WHERE score_date >= COALESCE(
                DATEADD(day, -2, (SELECT MAX(score_date) FROM datascience_public_misc.score_analytics.chain_score_distribution)),
                '2024-01-01'
            )
            GROUP BY chain, score_date, total_score

            UNION ALL

            SELECT 
                'base' as chain,
                score_date,
                total_score as score,
                COUNT(*) as count_with_score
            FROM datascience.onchain_scores.base
            WHERE score_date >= COALESCE(
                DATEADD(day, -2, (SELECT MAX(score_date) FROM datascience_public_misc.score_analytics.chain_score_distribution)),
                '2024-01-01'
            )
            GROUP BY chain, score_date, total_score

            UNION ALL

            SELECT 
                'blast' as chain,
                score_date,
                total_score as score,
                COUNT(*) as count_with_score
            FROM datascience.onchain_scores.blast
            WHERE score_date >= COALESCE(
                DATEADD(day, -2, (SELECT MAX(score_date) FROM datascience_public_misc.score_analytics.chain_score_distribution)),
                '2024-01-01'
            )
            GROUP BY chain, score_date, total_score

            UNION ALL

            SELECT 
                'bsc' as chain,
                score_date,
                total_score as score,
                COUNT(*) as count_with_score
            FROM datascience.onchain_scores.bsc
            WHERE score_date >= COALESCE(
                DATEADD(day, -2, (SELECT MAX(score_date) FROM datascience_public_misc.score_analytics.chain_score_distribution)),
                '2024-01-01'
            )
            GROUP BY chain, score_date, total_score

            UNION ALL
            

            SELECT 
                'ethereum' as chain,
                score_date,
                total_score as score,
                COUNT(*) as count_with_score
            FROM datascience.onchain_scores.ethereum
            WHERE score_date >= COALESCE(
                DATEADD(day, -2, (SELECT MAX(score_date) FROM datascience_public_misc.score_analytics.chain_score_distribution)),
                '2024-01-01'
            )
            GROUP BY chain, score_date, total_score

            UNION ALL

            SELECT 
                'flow' as chain,
                score_date,
                total_score as score,
                COUNT(*) as count_with_score
            FROM datascience.onchain_scores.flow
            WHERE score_date >= COALESCE(
                DATEADD(day, -2, (SELECT MAX(score_date) FROM datascience_public_misc.score_analytics.chain_score_distribution)),
                '2024-01-01'
            )
            GROUP BY chain, score_date, total_score

            UNION ALL

            SELECT 
                'kaia' as chain,
                score_date,
                total_score as score,
                COUNT(*) as count_with_score
            FROM datascience.onchain_scores.kaia
            WHERE score_date >= COALESCE(
                DATEADD(day, -2, (SELECT MAX(score_date) FROM datascience_public_misc.score_analytics.chain_score_distribution)),
                '2024-01-01'
            )
            GROUP BY chain, score_date, total_score

            UNION ALL

            SELECT 
                'near' as chain,
                score_date,
                total_score as score,
                COUNT(*) as count_with_score
            FROM datascience.onchain_scores.near
            WHERE score_date >= COALESCE(
                DATEADD(day, -2, (SELECT MAX(score_date) FROM datascience_public_misc.score_analytics.chain_score_distribution)),
                '2024-01-01'
            )
            GROUP BY chain, score_date, total_score

            UNION ALL

            SELECT 
                'optimism' as chain,
                score_date,
                total_score as score,
                COUNT(*) as count_with_score
            FROM datascience.onchain_scores.optimism
            WHERE score_date >= COALESCE(
                DATEADD(day, -2, (SELECT MAX(score_date) FROM datascience_public_misc.score_analytics.chain_score_distribution)),
                '2024-01-01'
            )
            GROUP BY chain, score_date, total_score

            UNION ALL

            SELECT 
                'polygon' as chain,
                score_date,
                total_score as score,
                COUNT(*) as count_with_score
            FROM datascience.onchain_scores.polygon
            WHERE score_date >= COALESCE(
                DATEADD(day, -2, (SELECT MAX(score_date) FROM datascience_public_misc.score_analytics.chain_score_distribution)),
                '2024-01-01'
            )
            GROUP BY chain, score_date, total_score

            UNION ALL

            SELECT 
                'sei' as chain,
                score_date,
                total_score as score,
                COUNT(*) as count_with_score
            FROM datascience.onchain_scores.sei
            WHERE score_date >= COALESCE(
                DATEADD(day, -2, (SELECT MAX(score_date) FROM datascience_public_misc.score_analytics.chain_score_distribution)),
                '2024-01-01'
            )
            GROUP BY chain, score_date, total_score

            UNION ALL

            SELECT 
                'solana' as chain,
                score_date,
                total_score as score,
                COUNT(*) as count_with_score
            FROM datascience.onchain_scores.solana
            WHERE score_date >= COALESCE(
                DATEADD(day, -2, (SELECT MAX(score_date) FROM datascience_public_misc.score_analytics.chain_score_distribution)),
                '2024-01-01'
            )
            GROUP BY chain, score_date, total_score

            UNION ALL

            SELECT 
                'thorchain' as chain,
                score_date,
                total_score as score,
                COUNT(*) as count_with_score
            FROM datascience.onchain_scores.thorchain
            WHERE score_date >= COALESCE(
                DATEADD(day, -2, (SELECT MAX(score_date) FROM datascience_public_misc.score_analytics.chain_score_distribution)),
                '2024-01-01'
            )
            GROUP BY chain, score_date, total_score
        )
        SELECT 
            chain,
            score_date,
            score,
            count_with_score
        FROM base_scores
    ) AS source
    ON target.chain = source.chain 
    AND target.score_date = source.score_date
    AND target.score = source.score
    WHEN MATCHED THEN
        UPDATE SET 
            count_with_score = source.count_with_score,
            updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (chain, score_date, score, count_with_score, updated_at)
        VALUES (
            source.chain, 
            source.score_date, 
            source.score, 
            source.count_with_score,
            CURRENT_TIMESTAMP()
        );

    RETURN 'Chain score distribution updated successfully';
END;
$$;

-- Create task to update chain score distribution every 12 hours
CREATE OR REPLACE TASK datascience_public_misc.score_analytics.update_chain_score_distribution_task
  WAREHOUSE = 'DATA_SCIENCE'
  SCHEDULE = 'USING CRON 0 */12 * * * America/Los_Angeles'
AS
  CALL datascience_public_misc.score_analytics.update_chain_score_distribution();

-- Resume the task (tasks are created in suspended state by default)
ALTER TASK datascience_public_misc.score_analytics.update_chain_score_distribution_task RESUME;

-- Step 5: Add clustering to the table
ALTER TABLE datascience_public_misc.score_analytics.chain_score_distribution
CLUSTER BY (chain, score_date);

-- Step 6: Set appropriate permissions
GRANT USAGE ON SCHEMA datascience_public_misc.score_analytics TO ROLE INTERNAL_DEV;
GRANT ALL PRIVILEGES ON TABLE datascience_public_misc.score_analytics.chain_score_distribution TO ROLE INTERNAL_DEV;

-- Individual access 
GRANT USAGE ON DATABASE datascience_public_misc TO ROLE VELOCITY_ETHEREUM;
GRANT USAGE ON SCHEMA datascience_public_misc.score_analytics TO ROLE VELOCITY_ETHEREUM;
GRANT SELECT ON TABLE datascience_public_misc.score_analytics.chain_score_distribution TO ROLE VELOCITY_ETHEREUM;