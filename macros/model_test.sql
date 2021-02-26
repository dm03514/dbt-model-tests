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

{% macro source(model_name) %}
  {% set dbt_model_test_enabled = env_var('DBT_MODEL_TEST_ENABLED', False) %}
  {% set dbt_model_test_identifier_prefix = env_var('DBT_MODEL_TEST_IDENTIFIER_PREFIX', '') %}

  {{ log("Running custom:source " ~ model_name) }}

  {% if dbt_model_test_enabled == '1' %}
    {{ log("DBT_MODEL_TEST: model test enabled") }}
    {% set rel = builtins.source(model_name) %}
    {%
      set newrel = rel.replace_path(
        identifier=dbt_model_test_identifier_prefix + model_name
      )
    %}
    {% do return(newrel) %}
  {% else %}
    {{ log("DBT_MODEL_TEST: model test disabled") }}
    {% do return(builtins.source(model_name)) %}
  {% endif %}
{% endmacro %}
