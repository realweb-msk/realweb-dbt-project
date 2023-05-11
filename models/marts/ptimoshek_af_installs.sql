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

SELECT * FROM {{ ref("stg_facebook_2" )}}
UNION ALL
SELECT * FROM {{ ref("stg_google_ads_2" )}}
UNION ALL
SELECT * FROM {{ ref("stg_huawei_ads_2" )}}
UNION ALL
SELECT * FROM {{ ref("stg_mytarget_2" )}}
UNION ALL
SELECT * FROM {{ ref("stg_tiktok_2" )}}
UNION ALL
SELECT * FROM {{ ref("stg_vkontakte_2" )}}
UNION ALL
SELECT * FROM {{ ref("stg_yandex_2" )}}