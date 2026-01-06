# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS fmcg;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC USE catalog fmcg;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS fmcg.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES FROM fmcg;

# COMMAND ----------

# %sql
# DROP CATALOG IF EXISTS ecommerce CASCADE;

# COMMAND ----------

# MAGIC %md
# MAGIC **Create Bronze and Silver schemas for child company**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS fmcg.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS fmcg.silver;