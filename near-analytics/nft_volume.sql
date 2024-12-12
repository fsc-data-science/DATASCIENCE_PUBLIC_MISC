
select 
block_timestamp, block_id, tx_hash, receipt_id,
platform_name, nft_address, token_id, price, 
affiliate_id, affiliate_amount,
platform_fee,  
royalties, array_size(object_keys(royalties)) as n_royalties
from near.nft.ez_nft_sales 
where block_timestamp >= '2024-12-01'
having n_royalties > 0
;

with base_data as (
    select 
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
    from near.nft.ez_nft_sales s
    where s.block_timestamp >= '2024-11-01'
),

no_royalties as (
    select 
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
    from base_data
    where n_royalties = 0
),

single_royalty as (
    select 
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
    from base_data
    where n_royalties = 1
),

multi_royalties_detail as (
    select 
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
        row_number() over (partition by b.receipt_id, b.token_id 
                          order by royalty_percent desc) as royalty_index
    from base_data b,
        lateral flatten(input => royalties) f
    where b.n_royalties > 1
),

multi_royalties_agg as (
    select 
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
    from multi_royalties_detail
    where royalty_index > 1
    group by receipt_id, token_id, royalties
),

combined_ as (
select *, 0 as n_royalties, 
from no_royalties
union all
select *, 0 as n_royalties
from single_royalty
union all
select *, array_size(object_keys(royalties))-1 as n_royalties
from multi_royalties_agg
order by receipt_id, token_id
)

select 
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
from combined_
;