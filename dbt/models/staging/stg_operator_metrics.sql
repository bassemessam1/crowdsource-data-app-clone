with source as (

    select * from {{ source('gold', 'operator_metrics') }}

),

renamed as (

    select
        operator_name,

        -- year/month/day/hour are unpadded strings (e.g. month = '9', not '09')
        -- straight from the Spark job's int-to-string cast — pad before parsing.
        timestamp(
            concat(
                cast(year as string), '-',
                lpad(cast(month as string), 2, '0'), '-',
                lpad(cast(day as string), 2, '0'), ' ',
                lpad(cast(hour as string), 2, '0'), ':00:00'
            )
        ) as metric_hour,

        date(
            concat(
                cast(year as string), '-',
                lpad(cast(month as string), 2, '0'), '-',
                lpad(cast(day as string), 2, '0')
            )
        ) as metric_date,

        cast(measurement_count as int64) as measurement_count,
        avg_download_mbps,
        avg_upload_mbps,
        avg_latency_ms,
        p50_download_mbps,
        p90_download_mbps,
        stddev_download_mbps

    from source

)

select * from renamed
