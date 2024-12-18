with nft_first_transfers AS (
select contract_address, 
SPLIT_PART(contract_address, '.', -2) as launch_platform,
min(block_timestamp) as first_transfer
from near.nft.fact_nft_transfers 
group by contract_address, launch_platform
)

select 
case when length(launch_platform) < 1 then 'independent developer' else 
launch_platform end as launch_platform, 
count(1) as n_ 
from nft_first_transfers
group by launch_platform 
order by n_ desc;
