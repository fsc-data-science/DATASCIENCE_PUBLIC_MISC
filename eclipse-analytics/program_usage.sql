select * from datascience_public_misc.eclipse_analytics.program_usage;

-- Step 1: Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS datascience_public_misc.eclipse_analytics;

-- Step 2: Create table based on observation level (program-bucket level)
CREATE OR REPLACE TABLE datascience_public_misc.eclipse_analytics.program_usage (
    program_bucket VARCHAR,
    program_bucket_short VARCHAR,
    num_signers INTEGER,
    n_tx INTEGER
);

-- Step 3: Call the procedure
CALL datascience_public_misc.eclipse_analytics.update_program_usage();

-- Step 4: Define the procedure
CREATE OR REPLACE PROCEDURE datascience_public_misc.eclipse_analytics.update_program_usage()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    -- Truncate the table before inserting new data
    TRUNCATE TABLE datascience_public_misc.eclipse_analytics.program_usage;
    
    -- Insert new data
    INSERT INTO datascience_public_misc.eclipse_analytics.program_usage
    WITH excluded_programs AS (
        SELECT address FROM (VALUES
            ('BPFLoaderUpgradeab1e11111111111111111111111'),
            ('ComputeBudget111111111111111111111111111111'),
            ('11111111111111111111111111111111'),
            ('TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb'),
            ('ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL'),
            ('TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'),
            ('FUCHhfHbuxXBWiRBfVdhmiog84sUJw11aAq3ibAUGL6e'),
            ('5hEa5j38yNJRM9vQA44Q6gXVj4Db8y3mWxkDtQeofKKs')
        ) AS t(address)
    ),
    
    user_programs AS (
        SELECT 
            signers[0] as signer,
            COUNT(DISTINCT program_id) as programs_used,
            COUNT(DISTINCT tx_id) as n_tx
        FROM eclipse.core.fact_events e 
        LEFT OUTER JOIN excluded_programs p ON e.program_id = p.address
        WHERE signers[0] != 'G5FM3UKwcBJ47PwLWLLY1RQpqNtTMgnqnd6nZGcJqaBp'
        GROUP BY 1
    ),
    
    bucketed_users AS (
        SELECT
            CASE 
                WHEN programs_used = 1 THEN '1. One Program'
                WHEN programs_used = 2 THEN '2. Two Programs'
                WHEN programs_used <= 4 THEN '3. 3-4 Programs'
                WHEN programs_used <= 6 THEN '4. 5-6 Programs'
                WHEN programs_used <= 9 THEN '5. 7-9 Programs'
                ELSE '6. 10+ Programs'
            END as program_bucket,
            CASE 
                WHEN programs_used = 1 THEN '1'
                WHEN programs_used = 2 THEN '2'
                WHEN programs_used <= 4 THEN '3-4'
                WHEN programs_used <= 6 THEN '5-6'
                WHEN programs_used <= 9 THEN '7-9'
                ELSE '10+'
            END as program_bucket_short,
            COUNT(signer) as num_signers,
            SUM(n_tx) as n_tx 
        FROM user_programs
        GROUP BY 1, 2
    )
    
    SELECT 
        program_bucket,
        program_bucket_short,
        num_signers,
        n_tx
    FROM bucketed_users
    ORDER BY program_bucket;
    
    RETURN 'Eclipse program usage statistics updated successfully';
END;
$$;

-- Create task to update every 12 hours
CREATE OR REPLACE TASK datascience_public_misc.eclipse_analytics.update_program_usage_task
    WAREHOUSE = 'DATA_SCIENCE'
    SCHEDULE = 'USING CRON 0 */12 * * * America/Los_Angeles'
AS
    CALL datascience_public_misc.eclipse_analytics.update_program_usage();

-- Resume the task
ALTER TASK datascience_public_misc.eclipse_analytics.update_program_usage_task RESUME;

-- Grant schema usage
GRANT USAGE ON SCHEMA datascience_public_misc.eclipse_analytics TO ROLE INTERNAL_DEV;
GRANT ALL PRIVILEGES ON TABLE datascience_public_misc.eclipse_analytics.program_usage TO ROLE INTERNAL_DEV;

-- Individual access for Velocity Ethereum
GRANT USAGE ON DATABASE datascience_public_misc TO ROLE VELOCITY_ETHEREUM;
GRANT USAGE ON SCHEMA datascience_public_misc.eclipse_analytics TO ROLE VELOCITY_ETHEREUM;
GRANT SELECT ON TABLE datascience_public_misc.eclipse_analytics.program_usage TO ROLE VELOCITY_ETHEREUM;