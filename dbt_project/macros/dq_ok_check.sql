{% macro dq_ok_check(country_constraint, supplier_constraint) %}

(distinct_companies_count >= {{ country_constraint }} and distinct_suppliers_count >= {{ supplier_constraint }})

{% endmacro %}
