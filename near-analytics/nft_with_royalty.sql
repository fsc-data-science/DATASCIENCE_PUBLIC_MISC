select * from datascience_public_misc.near_analytics.nft_sales_with_royalty
limit 10;

-- Step 1: Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS datascience_public_misc.near_analytics;

-- Step 2: Create table based on observation level (transaction level)
CREATE OR REPLACE TABLE datascience_public_misc.near_analytics.nft_sales_with_royalty (
    block_timestamp TIMESTAMP,
    block_id NUMBER,
    tx_hash STRING,
    receipt_id STRING,
    platform_name STRING,
    nft_address STRING,
    token_id STRING,
    price FLOAT,
    affiliate_id STRING,
    affiliate_amount FLOAT,
    platform_fee FLOAT,
    royalty_amount_near FLOAT,
    royalty_percent FLOAT,
    total_fees_amount FLOAT,
    royalties VARIANT,
    n_royalties INTEGER
);

-- Step 3: Call the procedure
CALL datascience_public_misc.near_analytics.update_nft_sales_with_royalty();

-- Step 4: Define the procedure with lookback period
CREATE OR REPLACE PROCEDURE datascience_public_misc.near_analytics.update_nft_sales_with_royalty()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    MERGE INTO datascience_public_misc.near_analytics.nft_sales_with_royalty AS target
    USING (
        WITH base_data AS (
            SELECT 
                s.block_timestamp, 
                s.block_id, 
                s.tx_hash, 
                s.receipt_id,
                s.platform_name, 
                s.nft_address, 
                s.token_id, 
                s.price, 
                s.affiliate_id, 
                s.affiliate_amount,
                s.platform_fee,  
                s.royalties,
                array_size(object_keys(s.royalties)) as n_royalties
            FROM near.nft.ez_nft_sales s
            WHERE s.block_timestamp >= COALESCE(
                DATEADD(day, -2, (SELECT MAX(block_timestamp) FROM datascience_public_misc.near_analytics.nft_sales_with_royalty)),
                '1970-01-01'
            )
        ),

        no_royalties AS (
            SELECT 
                block_timestamp, 
                block_id, 
                tx_hash, 
                receipt_id,
                platform_name, 
                nft_address, 
                token_id, 
                price, 
                affiliate_id, 
                affiliate_amount,
                platform_fee,  
                royalties,
                0 as royalty_amount_near,
                0 as royalty_percent
            FROM base_data
            WHERE n_royalties = 0
        ),

        single_royalty AS (
            SELECT 
                block_timestamp, 
                block_id, 
                tx_hash, 
                receipt_id,
                platform_name, 
                nft_address, 
                token_id, 
                price, 
                affiliate_id, 
                affiliate_amount,
                platform_fee,  
                royalties,
                0 as royalty_amount_near,
                0 as royalty_percent
            FROM base_data
            WHERE n_royalties = 1
        ),

        multi_royalties_detail AS (
            SELECT 
                b.block_timestamp, 
                b.block_id, 
                b.tx_hash, 
                b.receipt_id,
                b.platform_name, 
                b.nft_address, 
                b.token_id, 
                b.price, 
                b.affiliate_id, 
                b.affiliate_amount,
                b.platform_fee,  
                b.royalties,
                DIV0(f.value::float, 1e24) as royalty_amount_near,
                DIV0(DIV0(f.value::float, 1e24), b.price) as royalty_percent,
                row_number() OVER (PARTITION BY b.receipt_id, b.token_id 
                                 ORDER BY royalty_percent DESC) as royalty_index
            FROM base_data b,
                lateral flatten(input => royalties) f
            WHERE b.n_royalties > 1
        ),

        multi_royalties_agg AS (
            SELECT 
                max(block_timestamp) as block_timestamp,
                max(block_id) as block_id,
                max(tx_hash) as tx_hash,
                receipt_id,
                max(platform_name) as platform_name,
                max(nft_address) as nft_address,
                token_id,
                max(price) as price,
                max(affiliate_id) as affiliate_id,
                max(affiliate_amount) as affiliate_amount,
                max(platform_fee) as platform_fee,
                royalties,
                sum(royalty_amount_near) as royalty_amount_near,
                DIV0(sum(royalty_amount_near), max(price)) as royalty_percent
            FROM multi_royalties_detail
            WHERE royalty_index > 1
            GROUP BY receipt_id, token_id, royalties
        ),

        combined_ AS (
            SELECT *, 0 as n_royalties
            FROM no_royalties
            UNION ALL
            SELECT *, 0 as n_royalties
            FROM single_royalty
            UNION ALL
            SELECT *, array_size(object_keys(royalties))-1 as n_royalties
            FROM multi_royalties_agg
        )

        SELECT 
            block_timestamp,
            block_id,
            tx_hash,
            receipt_id,
            platform_name,
            nft_address,
            token_id,
            COALESCE(price, 0) as price,
            affiliate_id,
            COALESCE(affiliate_amount, 0) as affiliate_amount,
            COALESCE(platform_fee, 0) as platform_fee,
            COALESCE(royalty_amount_near, 0) as royalty_amount_near,
            COALESCE(royalty_percent, 0) as royalty_percent,
            COALESCE(royalty_amount_near, 0) + 
            COALESCE(affiliate_amount, 0) +
            COALESCE(platform_fee, 0) as total_fees_amount,
            royalties,
            n_royalties
        FROM combined_
    ) AS source
    ON target.receipt_id = source.receipt_id 
    AND target.token_id = source.token_id
    WHEN MATCHED THEN
        UPDATE SET 
            block_timestamp = source.block_timestamp,
            block_id = source.block_id,
            tx_hash = source.tx_hash,
            platform_name = source.platform_name,
            nft_address = source.nft_address,
            price = source.price,
            affiliate_id = source.affiliate_id,
            affiliate_amount = source.affiliate_amount,
            platform_fee = source.platform_fee,
            royalty_amount_near = source.royalty_amount_near,
            royalty_percent = source.royalty_percent,
            total_fees_amount = source.total_fees_amount,
            royalties = source.royalties,
            n_royalties = source.n_royalties
    WHEN NOT MATCHED THEN
        INSERT (
            block_timestamp, block_id, tx_hash, receipt_id, platform_name, 
            nft_address, token_id, price, affiliate_id, affiliate_amount, 
            platform_fee, royalty_amount_near, royalty_percent, 
            total_fees_amount, royalties, n_royalties
        )
        VALUES (
            source.block_timestamp, source.block_id, source.tx_hash, 
            source.receipt_id, source.platform_name, source.nft_address, 
            source.token_id, source.price, source.affiliate_id, 
            source.affiliate_amount, source.platform_fee, 
            source.royalty_amount_near, source.royalty_percent, 
            source.total_fees_amount, source.royalties, source.n_royalties
        );

    RETURN 'NFT sales with royalty data updated successfully';
END;
$$;

-- Add clustering to improve query performance
ALTER TABLE datascience_public_misc.near_analytics.nft_sales_with_royalty
CLUSTER BY (block_timestamp, receipt_id);

-- Set appropriate permissions
GRANT USAGE ON SCHEMA datascience_public_misc.near_analytics TO ROLE INTERNAL_DEV;
GRANT ALL PRIVILEGES ON TABLE datascience_public_misc.near_analytics.nft_sales_with_royalty TO ROLE INTERNAL_DEV;

-- Grant Studio access
GRANT USAGE ON DATABASE datascience_public_misc TO ROLE VELOCITY_ETHEREUM;
GRANT USAGE ON SCHEMA datascience_public_misc.near_analytics TO ROLE VELOCITY_ETHEREUM;
GRANT SELECT ON TABLE datascience_public_misc.near_analytics.nft_sales_with_royalty TO ROLE VELOCITY_ETHEREUM;