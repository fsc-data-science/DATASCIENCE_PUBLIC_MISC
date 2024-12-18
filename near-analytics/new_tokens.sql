select 
date_trunc('month', first_transfer) as month_, 
count(1) as num_tokens
 from datascience_public_misc.near_analytics.token_first_transfers
 group by month_
 order by month_ asc;
 

-- Step 1: Create schema
CREATE SCHEMA IF NOT EXISTS datascience_public_misc.near_analytics;

-- Step 2: Create table for token first transfers
CREATE TABLE IF NOT EXISTS datascience_public_misc.near_analytics.token_first_transfers (
    contract_address VARCHAR,
    first_transfer TIMESTAMP
);

-- Step 3: Call the procedure
CALL datascience_public_misc.near_analytics.update_token_first_transfers();

-- Step 4: Define procedure
CREATE OR REPLACE PROCEDURE datascience_public_misc.near_analytics.update_token_first_transfers()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    MERGE INTO datascience_public_misc.near_analytics.token_first_transfers AS target
    USING (
        SELECT 
            t.contract_address,
            MIN(t.block_timestamp) as first_transfer
        FROM datascience_public_misc.near_analytics.token_first_transfers existing
        RIGHT JOIN near.core.ez_token_transfers t
            ON existing.contract_address = t.contract_address
        WHERE existing.contract_address IS NULL  -- Only process new tokens
        GROUP BY t.contract_address
    ) AS source
    ON target.contract_address = source.contract_address
    WHEN NOT MATCHED THEN
        INSERT (contract_address, first_transfer)
        VALUES (source.contract_address, source.first_transfer);

    RETURN 'New token first transfers discovered and recorded successfully';
END;
$$;

-- Add clustering to improve query performance
ALTER TABLE datascience_public_misc.near_analytics.token_first_transfers
CLUSTER BY (contract_address, first_transfer);

-- Set appropriate permissions
GRANT USAGE ON SCHEMA datascience_public_misc.near_analytics TO ROLE INTERNAL_DEV;
GRANT ALL PRIVILEGES ON TABLE datascience_public_misc.near_analytics.token_first_transfers TO ROLE INTERNAL_DEV;

-- Individual access 
GRANT USAGE ON DATABASE datascience_public_misc TO ROLE VELOCITY_ETHEREUM;
GRANT USAGE ON SCHEMA datascience_public_misc.near_analytics TO ROLE VELOCITY_ETHEREUM;
GRANT SELECT ON TABLE datascience_public_misc.near_analytics.token_first_transfers TO ROLE VELOCITY_ETHEREUM;

