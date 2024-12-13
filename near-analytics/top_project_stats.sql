select * from datascience_public_misc.near_analytics.near_project_list;
select * from datascience_public_misc.near_analytics.near_daily_project_stats
order by day_ asc;

-- Step 1: Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS datascience_public_misc.near_analytics;

-- Step 2: Create and populate project list table
CREATE OR REPLACE TABLE datascience_public_misc.near_analytics.near_project_list (
    project_name VARCHAR
);

-- Refresh project list with top 100 projects
TRUNCATE TABLE datascience_public_misc.near_analytics.near_project_list;
INSERT INTO datascience_public_misc.near_analytics.near_project_list (project_name)
SELECT project_name
FROM (
    SELECT
        l.project_name as project_name,
        COUNT(1) as n_calls --not unique tx hashes
    FROM near.core.fact_actions_events_function_call f
    JOIN near.core.dim_address_labels l
        ON f.receiver_id = l.address 
    WHERE f.receipt_succeeded = true
        AND l.label_type NOT IN ('cex', 'token') 
        AND l.project_name IS NOT NULL
        AND f.block_timestamp >= '2024-11-01'
        AND f.block_timestamp < '2024-12-01'
    GROUP BY 1
    ORDER BY n_calls DESC
    LIMIT 100
);

-- Step 3: Create daily stats table with observation level (project-day)
CREATE OR REPLACE TABLE datascience_public_misc.near_analytics.near_daily_project_stats (
    day_ DATE,
    project_name VARCHAR,
    n_contract_calls INTEGER,
    n_tx INTEGER,
    n_unique_signers INTEGER,
    volume_near FLOAT
);

-- Step 4: Call the procedure
CALL datascience_public_misc.near_analytics.update_near_daily_project_stats();

-- Step 5: Define the procedure with project list filter
CREATE OR REPLACE PROCEDURE datascience_public_misc.near_analytics.update_near_daily_project_stats()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    MERGE INTO datascience_public_misc.near_analytics.near_daily_project_stats AS target
    USING (
        SELECT
            DATE_TRUNC('day', f.block_timestamp) AS day_,
            l.project_name as project_name,
            COUNT(*) as n_contract_calls,
            COUNT(DISTINCT f.tx_hash) as n_tx,
            COUNT(DISTINCT f.signer_id) as n_unique_signers,
            SUM(f.deposit/1e24) as volume_near
        FROM near.core.fact_actions_events_function_call f
        JOIN near.core.dim_address_labels l
            ON f.receiver_id = l.address 
        JOIN datascience_public_misc.near_analytics.near_project_list p  
            ON l.project_name = p.project_name
        WHERE f.receipt_succeeded = true
            AND l.label_type NOT IN ('cex', 'token') 
            AND f.block_timestamp >= COALESCE(
                DATEADD(day, -2, (SELECT MAX(day_) FROM datascience_public_misc.near_analytics.near_daily_project_stats)),
                '1970-01-01'
            )
        GROUP BY 1, 2
    ) AS source
    ON target.day_ = source.day_ 
        AND target.project_name = source.project_name
    WHEN MATCHED THEN
        UPDATE SET
            target.n_contract_calls = source.n_contract_calls,
            target.n_tx = source.n_tx,
            target.n_unique_signers = source.n_unique_signers,
            target.volume_near = source.volume_near
    WHEN NOT MATCHED THEN
        INSERT (
            day_, project_name, n_contract_calls, n_tx, 
            n_unique_signers, volume_near
        )
        VALUES (
            source.day_, source.project_name, source.n_contract_calls, 
            source.n_tx, source.n_unique_signers, source.volume_near
        );

    RETURN 'NEAR daily project stats updated successfully';
END;
$$;

-- Step 6: Add clustering to the table
ALTER TABLE datascience_public_misc.near_analytics.near_daily_project_stats
CLUSTER BY (day_, project_name);

-- Step 7: Create task to update project stats every 12 hours
CREATE OR REPLACE TASK datascience_public_misc.near_analytics.update_near_daily_project_stats_task
    WAREHOUSE = 'DATA_SCIENCE'
    SCHEDULE = 'USING CRON 0 */12 * * * America/Los_Angeles'
AS
    CALL datascience_public_misc.near_analytics.update_near_daily_project_stats();

-- Resume the task
ALTER TASK datascience_public_misc.near_analytics.update_near_daily_project_stats_task RESUME;

-- Step 8: Set appropriate permissions
GRANT USAGE ON DATABASE datascience_public_misc TO ROLE INTERNAL_DEV;
GRANT USAGE ON SCHEMA datascience_public_misc.near_analytics TO ROLE INTERNAL_DEV;
GRANT ALL PRIVILEGES ON TABLE datascience_public_misc.near_analytics.near_project_list TO ROLE INTERNAL_DEV;
GRANT ALL PRIVILEGES ON TABLE datascience_public_misc.near_analytics.near_daily_project_stats TO ROLE INTERNAL_DEV;

-- Grant Studio access
GRANT USAGE ON DATABASE datascience_public_misc TO ROLE VELOCITY_ETHEREUM;
GRANT USAGE ON SCHEMA datascience_public_misc.near_analytics TO ROLE VELOCITY_ETHEREUM;
GRANT SELECT ON TABLE datascience_public_misc.near_analytics.near_project_list TO ROLE VELOCITY_ETHEREUM;
GRANT SELECT ON TABLE datascience_public_misc.near_analytics.near_daily_project_stats TO ROLE VELOCITY_ETHEREUM;