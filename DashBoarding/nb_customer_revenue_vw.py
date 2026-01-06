# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW fmcg.gold.vw_customer_revenue AS
# MAGIC SELECT 
# MAGIC     customer as Customer, 
# MAGIC     SUM(total_amount_inr) AS Revenue,
# MAGIC     SUM(sold_quantity) as Quantity
# MAGIC FROM 
# MAGIC     fmcg.gold.vw_fact_orders_enriched
# MAGIC GROUP BY 
# MAGIC     customer
# MAGIC ORDER BY 
# MAGIC     Revenue DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW fmcg.gold.vw_product_revenue AS
# MAGIC SELECT
# MAGIC     product,
# MAGIC     price_inr,
# MAGIC     SUM(sold_quantity) AS total_sold_quantity,
# MAGIC     SUM(price_inr * sold_quantity) AS revenue
# MAGIC FROM  fmcg.gold.vw_fact_orders_enriched
# MAGIC GROUP BY product, price_inr;
# MAGIC

# COMMAND ----------

