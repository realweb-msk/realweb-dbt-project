WITH T AS (
       SELECT date,
              campaign_name,
              adset_name,
              platform,
              source,
              COUNT(*) AS installs
       FROM {{ ref("stg_af_installs") }}
       WHERE source = 'huawei' 
       GROUP BY date,
              campaign_name,
              adset_name,
              platform,
              source
),
T2 AS (
       SELECT *, 
       ROW_NUMBER() OVER (PARTITION BY date, campaign_name ORDER BY installs DESC) AS rn
       FROM T
)

SELECT date,
       campaign_name,
       adset_name,
       platform,
       source,
       installs
FROM T2
WHERE rn = 1