select * from datascience_public_misc.near_analytics.nft_collection_daily_metrics
where nft_address LIKE 'nft.%'
limit 50;

-- Step 1: Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS datascience_public_misc.near_analytics;

-- Step 2: Create table for daily collection metrics
CREATE OR REPLACE TABLE datascience_public_misc.near_analytics.nft_collection_daily_metrics (
    day_ DATE,
    nft_address STRING,
    collection_name STRING,
    nft_volume FLOAT,
    affiliate_volume FLOAT,
    platform_fee_volume FLOAT,
    royalty_volume FLOAT,
    n_tx INTEGER,
    n_receipts INTEGER
);

-- Step 3: Call the procedure
CALL datascience_public_misc.near_analytics.update_nft_collection_daily_metrics();

-- Step 4: Define the procedure with lookback period
CREATE OR REPLACE PROCEDURE datascience_public_misc.near_analytics.update_nft_collection_daily_metrics()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    MERGE INTO datascience_public_misc.near_analytics.nft_collection_daily_metrics AS target
    USING (
        SELECT 
            DATE_TRUNC('day', block_timestamp) as day_,
            nft_address,
            SPLIT_PART(REGEXP_REPLACE(nft_address, '^nft\\.', ''), '.', 1) as collection_name,
            SUM(price) as nft_volume,
            SUM(affiliate_amount) as affiliate_volume,
            SUM(platform_fee) as platform_fee_volume,
            SUM(royalty_amount_near) as royalty_volume,
            COUNT(DISTINCT tx_hash) as n_tx,
            COUNT(DISTINCT receipt_id) as n_receipts
        FROM datascience_public_misc.near_analytics.nft_sales_with_royalty
        WHERE DATE_TRUNC('day', block_timestamp) >= COALESCE(
            DATEADD(day, -2, (SELECT MAX(day_) FROM datascience_public_misc.near_analytics.nft_collection_daily_metrics)),
            '1970-01-01'
        )
        GROUP BY day_, nft_address, collection_name
    ) AS source
    ON target.day_ = source.day_ 
    AND target.nft_address = source.nft_address
    WHEN MATCHED THEN
        UPDATE SET 
            collection_name = source.collection_name,
            nft_volume = source.nft_volume,
            affiliate_volume = source.affiliate_volume,
            platform_fee_volume = source.platform_fee_volume,
            royalty_volume = source.royalty_volume,
            n_tx = source.n_tx,
            n_receipts = source.n_receipts
    WHEN NOT MATCHED THEN
        INSERT (
            day_,
            nft_address,
            collection_name,
            nft_volume,
            affiliate_volume,
            platform_fee_volume,
            royalty_volume,
            n_tx,
            n_receipts
        )
        VALUES (
            source.day_,
            source.nft_address,
            source.collection_name,
            source.nft_volume,
            source.affiliate_volume,
            source.platform_fee_volume,
            source.royalty_volume,
            source.n_tx,
            source.n_receipts
        );

    RETURN 'NFT collection daily metrics updated successfully';
END;
$$;

-- Add clustering to improve query performance
ALTER TABLE datascience_public_misc.near_analytics.nft_collection_daily_metrics
CLUSTER BY (day_, nft_address, collection_name);

-- Set appropriate permissions
GRANT USAGE ON SCHEMA datascience_public_misc.near_analytics TO ROLE INTERNAL_DEV;
GRANT ALL PRIVILEGES ON TABLE datascience_public_misc.near_analytics.nft_collection_daily_metrics TO ROLE INTERNAL_DEV;

-- Grant Studio access
GRANT USAGE ON DATABASE datascience_public_misc TO ROLE VELOCITY_ETHEREUM;
GRANT USAGE ON SCHEMA datascience_public_misc.near_analytics TO ROLE VELOCITY_ETHEREUM;
GRANT SELECT ON TABLE datascience_public_misc.near_analytics.nft_collection_daily_metrics TO ROLE VELOCITY_ETHEREUM;

-- Create task to update metrics every 12 hours
CREATE OR REPLACE TASK datascience_public_misc.near_analytics.update_nft_collection_daily_metrics_task
    WAREHOUSE = 'DATA_SCIENCE'
    SCHEDULE = 'USING CRON 0 */12 * * * America/Los_Angeles'
AS
    CALL datascience_public_misc.near_analytics.update_nft_collection_daily_metrics();

-- Resume the task
ALTER TASK datascience_public_misc.near_analytics.update_nft_collection_daily_metrics_task RESUME;