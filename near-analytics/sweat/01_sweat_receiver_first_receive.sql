-- For an address that has ever received from Sweat (see: 00_transfers_from_sweat.sql)

-- datascience_public_misc.near_analytics.sweat_welcome_transfers 


-- Identify the first time they received NEAR tokens EVER 
-- Flag whether it is their first receiving tx of native NEAR ever.


-- Those who receive from SWEAT but it is NOT their first time ever receiving NEAR tokens, do not count as welcome members.
-- see: 02_sweat_receiver_first_receive_criteria.sql for the filtering. 

select count(is_first_sweat_receive) total_receivers, 
sum(is_first_sweat_receive) as qualified_receivers,
total_receivers - qualified_receivers as non_qualified_receivers
from datascience_public_misc.near_analytics.qualified_sweat_users;

-- Step 1: Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS datascience_public_misc.near_analytics;

-- Step 2: Create table based on observation level
CREATE OR REPLACE TABLE datascience_public_misc.near_analytics.qualified_sweat_users (
    sweat_receiver VARCHAR,
    first_receive_timestamp TIMESTAMP,
    first_sweat_receive_timestamp TIMESTAMP,
    is_first_sweat_receive INTEGER
);

-- Step 3: Call the procedure
CALL datascience_public_misc.near_analytics.update_qualified_sweat_users();
-- Step 4: Define the procedure optimized with outer join to process only new addresses
CREATE OR REPLACE PROCEDURE datascience_public_misc.near_analytics.update_qualified_sweat_users()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    -- Insert only new records not already in the target table
    INSERT INTO datascience_public_misc.near_analytics.qualified_sweat_users
    (sweat_receiver, first_receive_timestamp, first_sweat_receive_timestamp, is_first_sweat_receive)
    WITH new_qualified_users AS (
        SELECT 
            to_address as sweat_receiver,
            min(nt.block_timestamp) as first_receive_timestamp,
            min(swt.block_timestamp) as first_sweat_receive_timestamp,
            case when min(nt.block_timestamp) = min(swt.block_timestamp) then 1 else 0 end as is_first_sweat_receive
        FROM near.core.ez_token_transfers nt 
        INNER JOIN datascience_public_misc.near_analytics.sweat_welcome_transfers swt
        ON nt.to_address = swt.sweat_receiver
        LEFT JOIN datascience_public_misc.near_analytics.qualified_sweat_users target
        ON swt.sweat_receiver = target.sweat_receiver
        WHERE target.sweat_receiver IS NULL -- Only process addresses not already in the target table
        GROUP BY to_address
    )
    SELECT * FROM new_qualified_users;

    RETURN 'NEAR qualified sweat users updated successfully - only new addresses processed';
END;
$$;

create or replace task datascience_public_misc.near_analytics.update_qualified_sweat_users_task
warehouse = 'DATA_SCIENCE'
schedule = 'USING CRON 0 */12 * * * America/Los_Angeles'
as
call datascience_public_misc.near_analytics.update_qualified_sweat_users();
alter task datascience_public_misc.near_analytics.update_qualified_sweat_users_task resume;


-- Step 5: Set appropriate permissions
GRANT USAGE ON SCHEMA datascience_public_misc.near_analytics TO ROLE INTERNAL_DEV;
GRANT ALL PRIVILEGES ON TABLE datascience_public_misc.near_analytics.qualified_sweat_users TO ROLE INTERNAL_DEV;

-- Individual access
GRANT USAGE ON DATABASE datascience_public_misc TO ROLE VELOCITY_ETHEREUM;
GRANT USAGE ON SCHEMA datascience_public_misc.near_analytics TO ROLE VELOCITY_ETHEREUM;
GRANT SELECT ON TABLE datascience_public_misc.near_analytics.qualified_sweat_users TO ROLE VELOCITY_ETHEREUM;