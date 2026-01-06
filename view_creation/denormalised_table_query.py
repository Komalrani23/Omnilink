# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW fmcg.gold.vw_fact_orders_enriched AS (
# MAGIC     SELECT 
# MAGIC         fo.date,
# MAGIC         fo.product_code,
# MAGIC         fo.customer_code,
# MAGIC
# MAGIC         -- Date attributes
# MAGIC         dd.date_key,
# MAGIC         dd.year,
# MAGIC         dd.month_name,
# MAGIC         dd.month_short_name,
# MAGIC         dd.quarter,
# MAGIC         dd.year_quarter,
# MAGIC
# MAGIC         -- Customer attributes
# MAGIC         dc.customer,
# MAGIC         dc.market,
# MAGIC         dc.platform,
# MAGIC         dc.channel,
# MAGIC
# MAGIC         -- Product attributes
# MAGIC         dp.division,
# MAGIC         dp.category,
# MAGIC         dp.product,
# MAGIC         dp.variant,
# MAGIC
# MAGIC         -- Metrics
# MAGIC         fo.sold_quantity,
# MAGIC         gp.price_inr,
# MAGIC
# MAGIC         -- Derived Metric: Amount
# MAGIC         (fo.sold_quantity * gp.price_inr) AS total_amount_inr
# MAGIC     
# MAGIC     FROM fmcg.gold.fact_orders fo
# MAGIC
# MAGIC     -- Join with Date Dimension
# MAGIC     LEFT JOIN fmcg.gold.dim_date dd
# MAGIC            ON fo.date = dd.month_start_date
# MAGIC
# MAGIC     -- Join with Customers
# MAGIC     LEFT JOIN fmcg.gold.dim_customers dc 
# MAGIC            ON fo.customer_code = dc.customer_code
# MAGIC
# MAGIC     -- Join with Products
# MAGIC     LEFT JOIN fmcg.gold.dim_products dp 
# MAGIC            ON fo.product_code = dp.product_code
# MAGIC
# MAGIC     -- Join with Price (year-based)
# MAGIC     LEFT JOIN fmcg.gold.dim_gross_price gp 
# MAGIC            ON fo.product_code = gp.product_code
# MAGIC           AND YEAR(fo.date) = gp.year
# MAGIC );
# MAGIC
# MAGIC -- Preview
# MAGIC SELECT * FROM fmcg.gold.vw_fact_orders_enriched;
# MAGIC

# COMMAND ----------

