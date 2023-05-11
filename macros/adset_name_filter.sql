{% macro adset_name_filter(adset_name) %}
    CASE {{adset_name}}
        WHEN '--' THEN ''
        WHEN '-' THEN ''
        ELSE {{adset_name}} END
{% endmacro %}