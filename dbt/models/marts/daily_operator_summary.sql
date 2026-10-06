with stg as (

    select * from {{ ref('stg_operator_metrics') }}

),

daily as (

    select
        operator_name,
        metric_date,
        sum(measurement_count)         as total_measurements,
        -- NOTE: these are unweighted averages of hourly averages, not
        -- re-derived from raw measurements. Fine for a learning project;
        -- a weighted average (by measurement_count) would be more correct
        -- if this were a real production metric.
        avg(avg_download_mbps)         as avg_download_mbps,
        avg(avg_upload_mbps)           as avg_upload_mbps,
        avg(avg_latency_ms)            as avg_latency_ms,
        avg(p50_download_mbps)         as avg_p50_download_mbps,
        avg(p90_download_mbps)         as avg_p90_download_mbps
    from stg
    group by operator_name, metric_date

)

select * from daily
