{# Fails on every row where the boolean expression is not true (null counts as a failure). #}
{% test expression_is_true(model, expression) %}
select * from {{ model }} where not coalesce(({{ expression }}), false)
{% endtest %}
