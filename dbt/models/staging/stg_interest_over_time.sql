with source as (
    select * from {{ source('gtrends_raw', 'raw_interest_over_time') }}
),

cleaned as (
    select
        cast(run_ts as timestamp)           as run_ts,
        upper(trim(geo))                    as geo,
        cast(timestamp as timestamp)        as trend_timestamp,
        date(cast(timestamp as timestamp))  as trend_date,
        lower(trim(keyword))                as keyword,
        cast(interest_value as int64)       as interest_value,
        cast(_ingested_at as timestamp)     as ingested_at
    from source
    where keyword is not null
      and interest_value is not null
)

select * from cleaned