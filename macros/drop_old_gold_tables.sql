{% macro drop_old_gold_tables() %}
  {% set old_tables = [
      'gold_daily_revenue',
      'gold_driver_performance',
      'gold_location_analytics',
      'gold_daily_driver_performance',
      'gold_daily_customer_analytics',
      'gold_daily_location_performance',
      'gold_daily_vehicle_fleet_metrics',
      'gold_daily_payment_reconciliation',
      '`(customer)_daily_metric`',
      '`(driver)`',
      '`(driver-performance)_daily_metric`',
      '`(location)_daily_metric`',
      '`(payment)_daily_reconciliation`',
      '`(revenue)_daily_metric`',
      '`(vehicle)_daily_metric`',
      'location_analytics'
  ] %}

  {% for tbl in old_tables %}
    {% set query %}
      DROP TABLE IF EXISTS pysparkdbt.gold.{{ tbl }}
    {% endset %}
    {% do run_query(query) %}
    {% do log("Dropped unused table: pysparkdbt.gold." ~ tbl, info=True) %}
  {% endfor %}
{% endmacro %}
