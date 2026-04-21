# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Transformación de Datos (Capa Silver)
# MAGIC
# MAGIC **Objetivo:** leer las tablas Bronze, aplicar limpieza, validación y
# MAGIC enriquecimiento para producir datos de alta calidad en la capa Silver.
# MAGIC
# MAGIC | Paso | Acción |
# MAGIC |------|--------|
# MAGIC | 1    | Leer tablas Bronze |
# MAGIC | 2    | Limpieza y deduplicación |
# MAGIC | 3    | Validación de calidad de datos |
# MAGIC | 4    | Enriquecimiento: joins y columnas derivadas |
# MAGIC | 5    | Escribir tablas Silver en Delta Lake |

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/flujo-databricks")

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, DateType

from config.config import (
    BRONZE_PATH, SILVER_PATH,
    BRONZE_ORDERS_TABLE, BRONZE_PRODUCTS_TABLE, BRONZE_CUSTOMERS_TABLE,
    SILVER_ORDERS_TABLE, SILVER_PRODUCTS_TABLE, SILVER_CUSTOMERS_TABLE,
    MIN_ORDER_AMOUNT, MAX_ORDER_AMOUNT, VALID_STATUS_VALUES,
)
from utils.helpers import (
    log_dataframe_stats,
    null_ratio_report,
    remove_duplicates,
    write_delta,
)

print("✅ Módulos importados correctamente")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Lectura desde la capa Bronze

# COMMAND ----------

df_orders_b    = spark.read.format("delta").load(f"{BRONZE_PATH}/orders")
df_products_b  = spark.read.format("delta").load(f"{BRONZE_PATH}/products")
df_customers_b = spark.read.format("delta").load(f"{BRONZE_PATH}/customers")

log_dataframe_stats(df_orders_b,    "bronze_orders")
log_dataframe_stats(df_products_b,  "bronze_products")
log_dataframe_stats(df_customers_b, "bronze_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Limpieza y deduplicación

# COMMAND ----------

# --- Órdenes ---
df_orders_clean = (
    df_orders_b
    # Eliminar columnas de metadatos de ingesta
    .drop("_ingestion_timestamp", "_source")
    # Quitar filas completamente nulas
    .dropna(how="all")
    # Deduplicar por ID de orden
    .dropDuplicates(["order_id"])
    # Normalizar tipos
    .withColumn("quantity",   F.col("quantity").cast(IntegerType()))
    .withColumn("unit_price", F.col("unit_price").cast(DoubleType()))
    .withColumn("order_date", F.to_date("order_date", "yyyy-MM-dd"))
    # Normalizar texto
    .withColumn("status",  F.lower(F.trim(F.col("status"))))
    .withColumn("country", F.upper(F.trim(F.col("country"))))
)
log_dataframe_stats(df_orders_clean, "orders_clean")

# COMMAND ----------

# --- Productos ---
df_products_clean = (
    df_products_b
    .drop("_ingestion_timestamp", "_source")
    .dropna(how="all")
    .dropDuplicates(["product_id"])
    .withColumn("unit_price", F.col("unit_price").cast(DoubleType()))
    .withColumn("category",   F.trim(F.col("category")))
)

# --- Clientes ---
df_customers_clean = (
    df_customers_b
    .drop("_ingestion_timestamp", "_source")
    .dropna(how="all")
    .dropDuplicates(["customer_id"])
    .withColumn("segment",           F.trim(F.col("segment")))
    .withColumn("country",           F.upper(F.trim(F.col("country"))))
    .withColumn("registration_date", F.to_date("registration_date", "yyyy-MM-dd"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Validación de calidad de datos

# COMMAND ----------

# Reporte de nulos
print("📋 Ratio de nulos — orders:")
for col, ratio in null_ratio_report(df_orders_clean).items():
    flag = "⚠️" if ratio > 0.01 else "✅"
    print(f"   {flag} {col:<20} {ratio:.2%}")

# COMMAND ----------

# Filtrar órdenes con valores inválidos
valid_statuses = F.col("status").isin(list(VALID_STATUS_VALUES))
valid_amount   = (F.col("quantity") * F.col("unit_price")).between(
    MIN_ORDER_AMOUNT, MAX_ORDER_AMOUNT
)
valid_keys     = F.col("order_id").isNotNull() & F.col("customer_id").isNotNull()

df_orders_valid = df_orders_clean.filter(valid_statuses & valid_amount & valid_keys)

rejected_count = df_orders_clean.count() - df_orders_valid.count()
print(f"🚫 Órdenes rechazadas por validación: {rejected_count:,}")
print(f"✅ Órdenes válidas: {df_orders_valid.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Enriquecimiento: joins y columnas derivadas

# COMMAND ----------

# Join órdenes + productos para añadir categoría y nombre de producto
df_enriched = (
    df_orders_valid
    .join(
        df_products_clean.select("product_id", "product_name", "category"),
        on="product_id",
        how="left",
    )
    .join(
        df_customers_clean.select("customer_id", "segment"),
        on="customer_id",
        how="left",
    )
)

# Columnas derivadas
df_silver_orders = (
    df_enriched
    .withColumn("total_amount",   F.round(F.col("quantity") * F.col("unit_price"), 2))
    .withColumn("year",           F.year("order_date"))
    .withColumn("month",          F.month("order_date"))
    .withColumn("day_of_week",    F.dayofweek("order_date"))
    .withColumn("is_cancelled",   (F.col("status") == "cancelled").cast("boolean"))
    .withColumn("_processed_at",  F.current_timestamp())
)

log_dataframe_stats(df_silver_orders, "silver_orders")
display(df_silver_orders.limit(5))

# COMMAND ----------

# Productos silver: añadir rango de precio
df_silver_products = (
    df_products_clean
    .withColumn(
        "price_range",
        F.when(F.col("unit_price") < 50,  "low")
         .when(F.col("unit_price") < 200, "medium")
         .otherwise("high"),
    )
    .withColumn("_processed_at", F.current_timestamp())
)

# Clientes silver: antigüedad en días
df_silver_customers = (
    df_customers_clean
    .withColumn("days_since_registration",
                F.datediff(F.current_date(), F.col("registration_date")))
    .withColumn("_processed_at", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Escritura en la capa Silver

# COMMAND ----------

write_delta(
    df_silver_orders,
    path=f"{SILVER_PATH}/orders",
    table_name=SILVER_ORDERS_TABLE,
    mode="overwrite",
    partition_by=["year", "month"],
)
print(f"✅ Tabla Silver: {SILVER_ORDERS_TABLE}")

# COMMAND ----------

write_delta(
    df_silver_products,
    path=f"{SILVER_PATH}/products",
    table_name=SILVER_PRODUCTS_TABLE,
    mode="overwrite",
)
print(f"✅ Tabla Silver: {SILVER_PRODUCTS_TABLE}")

# COMMAND ----------

write_delta(
    df_silver_customers,
    path=f"{SILVER_PATH}/customers",
    table_name=SILVER_CUSTOMERS_TABLE,
    mode="overwrite",
    partition_by=["country"],
)
print(f"✅ Tabla Silver: {SILVER_CUSTOMERS_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Capa Silver completada
# MAGIC
# MAGIC Continúa con el notebook **03_aggregation_gold** para generar las
# MAGIC métricas y KPIs de negocio.
