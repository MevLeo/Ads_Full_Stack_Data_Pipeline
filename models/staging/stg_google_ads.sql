with source as (

    select * from {{ source('bronze', 'google_ads_raw') }}

),

cleaned as (

    select
        -- identifiers
        campaign_id,
        company,
        'google_ads'                                    as platform,

        -- campaign attributes
        campaign_type,
        target_audience,
        customer_segment,
        location,
        language,
        cast(replace(duration, ' days', '') as integer) as duration_days,

        -- time
        cast(date as date)                              as date,

        -- volume metrics
        impressions,
        clicks,

        -- cost metrics
        spend_usd,
        round(spend_usd / nullif(clicks, 0), 4)        as cpc,

        -- engagement
        engagement_score,

        -- conversion metrics
        conversions,
        conversion_rate,
        round(clicks / nullif(impressions, 0), 4)      as ctr,

        -- return
        roi,

        -- metadata
        _loaded_at

    from source

    where campaign_type not in ('Email', 'Influencer')

)

select * from cleaned