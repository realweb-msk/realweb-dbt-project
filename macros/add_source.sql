{% macro add_source(table_name) %}
    SELECT 
        date,
        campaign_name,
        CASE 
            WHEN adset_name LIKE '-%' THEN CONCAT(SUBSTRING('{{table_name}}', 5, 10), '_ads')
            ELSE adset_name
        END AS adset_name,
        platform,
        SUBSTRING('{{table_name}}', 5, 10) as source,
        SUM(clicks) as clicks,
        SUM(impressions) as impressions,
        SUM(costs) as costs,
        NULL as installs
    FROM {{ref(table_name)}}
    WHERE 
        is_realweb 
        AND NOT is_ret_campaign
    GROUP BY 
        date,
        campaign_name,
        adset_name,
        platform
{% endmacro %}