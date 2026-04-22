# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Agregaciones de Negocio (Capa Gold)
# MAGIC
# MAGIC **Objetivo:** producir tablas de métricas y KPIs consolidados a partir
# MAGIC de los datos Silver, listos para consumo por herramientas de BI.
# MAGIC
# MAGIC | Tabla Gold                | Descripción |
# MAGIC |---------------------------|-------------|
# MAGIC | `gold_sales_summary`      | Ventas totales por mes y país |
# MAGIC | `gold_product_performance`| Top productos por ingresos y unidades |
# MAGIC | `gold_customer_segments`  | KPIs por segmento de cliente |

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/flujo-databricks")

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from config.config import (
    SILVER_PATH,
    GOLD_PATH,
    GOLD_SALES_SUMMARY_TABLE,
    GOLD_PRODUCT_PERFORMANCE_TABLE,
    GOLD_CUSTOMER_SEGMENTS_TABLE,
)
from utils.helpers import log_dataframe_stats, write_delta

print("✅ Módulos importados correctamente")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Lectura desde la capa Silver

# COMMAND ----------

df_orders    = spark.read.format("delta").load(f"{SILVER_PATH}/orders")
df_products  = spark.read.format("delta").load(f"{SILVER_PATH}/products")
df_customers = spark.read.format("delta").load(f"{SILVER_PATH}/customers")

print(f"Órdenes:   {df_orders.count():,} filas")
print(f"Productos: {df_products.count():,} filas")
print(f"Clientes:  {df_customers.count():,} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. `gold_sales_summary` — Resumen de ventas mensual por país

# COMMAND ----------

df_active_orders = df_orders.filter(~F.col("is_cancelled"))

df_sales_summary = (
    df_active_orders
    .groupBy("year", "month", "country")
    .agg(
        F.count("order_id").alias("total_orders"),
        F.sum("total_amount").alias("gross_revenue"),
        F.avg("total_amount").alias("avg_order_value"),
        F.sum("quantity").alias("total_units_sold"),
        F.countDistinct("customer_id").alias("unique_customers"),
    )
    .withColumn("gross_revenue",    F.round("gross_revenue", 2))
    .withColumn("avg_order_value",  F.round("avg_order_value", 2))
    .withColumn("year_month",
                F.concat_ws("-", F.col("year"), F.lpad(F.col("month"), 2, "0")))
    .withColumn("_computed_at", F.current_timestamp())
    .orderBy("year", "month", "country")
)

log_dataframe_stats(df_sales_summary, "gold_sales_summary")
display(df_sales_summary.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. `gold_product_performance` — Rendimiento de productos

# COMMAND ----------

# Ventas por producto
df_product_sales = (
    df_active_orders
    .groupBy("product_id", "product_name", "category")
    .agg(
        F.sum("total_amount").alias("total_revenue"),
        F.sum("quantity").alias("total_units"),
        F.count("order_id").alias("order_count"),
        F.avg("unit_price").alias("avg_price"),
        F.countDistinct("customer_id").alias("unique_buyers"),
    )
    .withColumn("total_revenue", F.round("total_revenue", 2))
    .withColumn("avg_price",     F.round("avg_price", 2))
)

# Ranking por ingresos dentro de cada categoría
window_cat = Window.partitionBy("category").orderBy(F.desc("total_revenue"))

df_product_performance = (
    df_product_sales
    .withColumn("rank_in_category", F.rank().over(window_cat))
    .withColumn("revenue_share",
                F.round(
                    F.col("total_revenue") /
                    F.sum("total_revenue").over(Window.partitionBy("category")) * 100,
                    2,
                ))
    .withColumn("_computed_at", F.current_timestamp())
    .orderBy("category", "rank_in_category")
)

log_dataframe_stats(df_product_performance, "gold_product_performance")
display(df_product_performance.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. `gold_customer_segments` — KPIs por segmento de cliente

# COMMAND ----------

# Unir órdenes con datos de cliente para obtener el segmento
df_orders_seg = (
    df_active_orders
    .join(df_customers.select("customer_id", "segment", "days_since_registration"),
          on="customer_id", how="left")
)

df_customer_segments = (
    df_orders_seg
    .groupBy("segment")
    .agg(
        F.countDistinct("customer_id").alias("customer_count"),
        F.count("order_id").alias("total_orders"),
        F.sum("total_amount").alias("total_revenue"),
        F.avg("total_amount").alias("avg_order_value"),
        F.avg("days_since_registration").alias("avg_seniority_days"),
        F.sum("quantity").alias("total_units"),
    )
    .withColumn("total_revenue",      F.round("total_revenue", 2))
    .withColumn("avg_order_value",    F.round("avg_order_value", 2))
    .withColumn("avg_seniority_days", F.round("avg_seniority_days", 1))
    .withColumn("revenue_per_customer",
                F.round(F.col("total_revenue") / F.col("customer_count"), 2))
    .withColumn("orders_per_customer",
                F.round(F.col("total_orders") / F.col("customer_count"), 2))
    .withColumn("_computed_at", F.current_timestamp())
    .orderBy(F.desc("total_revenue"))
)

log_dataframe_stats(df_customer_segments, "gold_customer_segments")
display(df_customer_segments)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Escritura en la capa Gold

# COMMAND ----------

write_delta(
    df_sales_summary,
    path=f"{GOLD_PATH}/sales_summary",
    table_name=GOLD_SALES_SUMMARY_TABLE,
    mode="overwrite",
    partition_by=["year", "month"],
)
print(f"✅ Tabla Gold: {GOLD_SALES_SUMMARY_TABLE}")

# COMMAND ----------

write_delta(
    df_product_performance,
    path=f"{GOLD_PATH}/product_performance",
    table_name=GOLD_PRODUCT_PERFORMANCE_TABLE,
    mode="overwrite",
    partition_by=["category"],
)
print(f"✅ Tabla Gold: {GOLD_PRODUCT_PERFORMANCE_TABLE}")

# COMMAND ----------

write_delta(
    df_customer_segments,
    path=f"{GOLD_PATH}/customer_segments",
    table_name=GOLD_CUSTOMER_SEGMENTS_TABLE,
    mode="overwrite",
)
print(f"✅ Tabla Gold: {GOLD_CUSTOMER_SEGMENTS_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Dashboard de KPIs globales

# COMMAND ----------

total_revenue = df_sales_summary.agg(F.sum("gross_revenue")).collect()[0][0]
total_orders  = df_sales_summary.agg(F.sum("total_orders")).collect()[0][0]
total_units   = df_sales_summary.agg(F.sum("total_units_sold")).collect()[0][0]
unique_cust   = df_orders.agg(F.countDistinct("customer_id")).collect()[0][0]

print("=" * 50)
print("         📊 KPIs GLOBALES DEL PERÍODO")
print("=" * 50)
print(f"  💰 Ingresos totales:    ${total_revenue:>12,.2f}")
print(f"  🛒 Órdenes procesadas:  {int(total_orders):>12,}")
print(f"  📦 Unidades vendidas:   {int(total_units):>12,}")
print(f"  👥 Clientes únicos:     {unique_cust:>12,}")
print(f"  🎯 Ticket promedio:     ${total_revenue/total_orders:>12,.2f}")
print("=" * 50)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Capa Gold completada
# MAGIC
# MAGIC Continúa con el notebook **04_ml_model** para entrenar un modelo
# MAGIC predictivo de ventas con MLflow.
