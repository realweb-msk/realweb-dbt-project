{% macro installs_counter(table_name) %}
    SELECT date,
        campaign_name,
        adset_name,
        platform,
        source,
        COUNT(DISTINCT appsflyer_id) AS installs
    FROM {{ref(table_name)}}
    WHERE 
        is_realweb 
        AND NOT is_ret_campaign
    GROUP BY 
        date,
        campaign_name,
        adset_name,
        platform, 
        source
{% endmacro %}