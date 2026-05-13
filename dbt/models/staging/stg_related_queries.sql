with source as (
    select * from {{ source('gtrends_raw', 'raw_related_queries') }}
),

cleaned as (
    select
        cast(run_ts as timestamp)       as run_ts,
        upper(trim(geo))                as geo,
        lower(trim(keyword))            as keyword,
        lower(trim(query_type))         as query_type,   -- 'top' or 'rising'
        lower(trim(related_query))      as related_query,
        value                           as score,
        cast(_ingested_at as timestamp) as ingested_at,
        date(cast(run_ts as timestamp)) as run_date
    from source
    where related_query is not null
)

select * from cleaned