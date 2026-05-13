-- Cleans and standardises raw trending searches from BigQuery raw layer.

with source as (
    select * from {{ source('gtrends_raw', 'raw_trending_searches') }}
),

cleaned as (
    select
        cast(run_ts as timestamp)           as run_ts,
        upper(trim(geo))                    as geo,
        lower(trim(keyword))                as keyword,
        cast(rank as int64)                 as rank,
        cast(_ingested_at as timestamp)     as ingested_at,
        date(cast(run_ts as timestamp))     as run_date
    from source
    where keyword is not null
      and geo is not null
)

select * from cleaned