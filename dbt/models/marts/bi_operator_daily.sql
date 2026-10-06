with daily as (

    select * from {{ ref('daily_operator_summary') }}

),

rankings as (

    select
        operator_name,
        metric_date,
        download_rank,
        upload_rank,
        latency_rank
    from {{ ref('operator_rankings') }}

),

joined as (

    select
        daily.operator_name,
        daily.metric_date,
        daily.total_measurements,
        daily.avg_download_mbps,
        daily.avg_upload_mbps,
        daily.avg_latency_ms,
        daily.avg_p50_download_mbps,
        daily.avg_p90_download_mbps,
        rankings.download_rank,
        rankings.upload_rank,
        rankings.latency_rank
    from daily
    left join rankings
        on daily.operator_name = rankings.operator_name
       and daily.metric_date = rankings.metric_date

)

select * from joined