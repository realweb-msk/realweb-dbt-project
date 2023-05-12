{% if target.name == 'prod' %}

{{
  config(
    materialized='table',
    partition_by = {
            "field": "date",
            "data_type": "date",
            "granularity": "day"
            }
  )
}}

{% endif %}

SELECT
    date,
    campaign_name,
    adset_name,
    platform,
    source,
    SUM(clicks) AS clicks,
    SUM(impressions) AS impressions, 
    SUM(installs) AS installs, 
    SUM(costs) AS costs
FROM {{ ref('stg_all_sources') }}
WHERE source != 'other'
GROUP BY 1,2,3,4,5
ORDER BY 1,2,3,4,5
