{% macro ref(model_name) %}
  {% set dbt_model_test_enabled = env_var('DBT_MODEL_TEST_ENABLED', False) %}
  {% set dbt_model_test_identifier_prefix = env_var('DBT_MODEL_TEST_IDENTIFIER_PREFIX', '') %}

  {{ log("Running custom:ref " ~ model_name) }}

  {% if dbt_model_test_enabled == '1' %}
    {{ log("DBT_MODEL_TEST: model test enabled") }}
    {% set rel = builtins.ref(model_name) %}
    {%
      set newrel = rel.replace_path(
        identifier=dbt_model_test_identifier_prefix + model_name
      )
    %}
    {% do return(newrel) %}
  {% else %}
    {{ log("DBT_MODEL_TEST: model test disabled") }}
    {% do return(builtins.ref(model_name)) %}
  {% endif %}
{% endmacro %}

{% macro source(source_name, table_name) %}
  {% set dbt_model_test_enabled = env_var('DBT_MODEL_TEST_ENABLED', False) %}
  {% set dbt_model_test_database = env_var('DBT_MODEL_TEST_DATABASE', '') %}
  {% set dbt_model_test_schema = env_var('DBT_MODEL_TEST_SCHEMA', '') %}
  {% set dbt_model_test_identifier_prefix = env_var('DBT_MODEL_TEST_IDENTIFIER_PREFIX', '') %}
  {{ log("Running custom:source " ~ source_name ~ "." ~ table_name) }}

  {% if dbt_model_test_enabled == '1' %}
    {{ log("DBT_MODEL_TEST: model test enabled") }}
    {% set rel = builtins.source(source_name, table_name) %}
    {%
      set newrel = rel.replace_path(
        database=dbt_model_test_database,
        schema=dbt_model_test_schema,
        identifier=dbt_model_test_identifier_prefix + table_name
      )
    %}
    {% do return(newrel) %}
  {% else %}
    {{ log("DBT_MODEL_TEST: model test disabled") }}
    {% do return(builtins.source(source_name, table_name)) %}
  {% endif %}
{% endmacro %}
