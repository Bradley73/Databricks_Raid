{% macro optimize_zorder(zorder_cols=[]) %}
  {% if zorder_cols | length > 0 %}
    OPTIMIZE {{ this }} ZORDER BY ({{ zorder_cols | join(', ') }})
  {% endif %}
{% endmacro %}

{{ config(
    post_hook = "{{ optimize_zorder(['col1','col2']) }}"
) }}
