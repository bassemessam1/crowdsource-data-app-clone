with daily as (

    select * from {{ ref('daily_operator_summary') }}

),

ranked as (

    select
        *,
        rank() over (partition by metric_date order by avg_download_mbps desc) as download_rank,
        rank() over (partition by metric_date order by avg_upload_mbps desc)   as upload_rank,
        rank() over (partition by metric_date order by avg_latency_ms asc)     as latency_rank
    from daily

)

select * from ranked
