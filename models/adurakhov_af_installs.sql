{% if target.name == 'prod' %}

{{
  config(
    materialized='table',
    partition_by = {
            "field": "date",
            "data_type": "date",
            "granularity": "day"
            },
    cluster_by = ["is_realweb", "is_ret_campaign", "source"]
  )
}}

{% endif %}

WITH source_t AS (
    SELECT DISTINCT
        event_time,
        Campaign,
        af_c_id, --не нужен
        af_adset,
        Media_Source,
        platform,
        --Event_Name, --в таблице только installs
        appsflyer_id
    FROM {% if source('vprok_for_dbt_ed', 'AF_installs') !=() %}
        {{ source('vprok_for_dbt_ed', 'AF_installs') }}
        {% else %}
        {{ref('stg_af_installs')}}
        {% endif %}
),

final AS (
    SELECT
        PARSE_DATE('%Y-%m-%d', REGEXP_EXTRACT(event_time, r'([\d-]*)\s')) date,
        PARSE_DATETIME('%Y-%m-%d %H:%M:%S', REGEXP_EXTRACT(event_time, r'(.*)\.\d{3}$')) AS event_time,
        {{ process_strings('campaign') }} AS campaign_name,
        {{ process_strings('af_adset') }} AS adset_name,
        {{ process_strings('media_source') }} AS media_source,
        {{ process_strings('platform') }} AS platform,
        appsflyer_id,
        {{ is_ret_campaign('campaign') }} is_ret_campaign,
        {{ is_realweb('campaign') }} is_realweb,
        {{ install_source('campaign') }} source,
        ROW_NUMBER() OVER(PARTITION BY appsflyer_id ORDER BY event_time) id_counter
    FROM source_t
)

SELECT
    date,
    event_time,
    campaign_name,
    adset_name,
    media_source,
    platform,
    appsflyer_id,
    is_ret_campaign,
    is_realweb,
    source
FROM final
WHERE id_counter = 1 AND is_realweb AND NOT is_ret_campaign AND (source!='other')
