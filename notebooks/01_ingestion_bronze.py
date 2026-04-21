# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Ingesta de Datos (Capa Bronze)
# MAGIC
# MAGIC **Objetivo:** leer los datos crudos de órdenes, productos y clientes
# MAGIC y almacenarlos *sin transformar* en tablas Delta Lake de la capa Bronze.
# MAGIC
# MAGIC | Paso | Acción |
# MAGIC |------|--------|
# MAGIC | 1    | Configurar entorno y rutas |
# MAGIC | 2    | Generar / leer datos de origen |
# MAGIC | 3    | Escribir tablas Bronze en Delta Lake |
# MAGIC | 4    | Registrar métricas de ingesta |

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/flujo-databricks")

from config.config import (
    DATABASE_NAME,
    BRONZE_PATH,
    BRONZE_ORDERS_TABLE,
    BRONZE_PRODUCTS_TABLE,
    BRONZE_CUSTOMERS_TABLE,
)
from utils.helpers import (
    generate_sample_orders,
    generate_sample_products,
    generate_sample_customers,
    log_dataframe_stats,
    write_delta,
)

print("✅ Módulos importados correctamente")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuración del entorno

# COMMAND ----------

# Crear la base de datos si no existe
spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")
print(f"📦 Base de datos '{DATABASE_NAME}' lista")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Generación de datos de origen
# MAGIC
# MAGIC En un entorno productivo estos datos provendrían de un sistema externo
# MAGIC (ERP, API, archivos en el data lake). Aquí se generan datos simulados
# MAGIC para propósitos de demostración.

# COMMAND ----------

# Órdenes de venta
df_orders_raw = generate_sample_orders(spark, n=2000)
log_dataframe_stats(df_orders_raw, "orders_raw")
display(df_orders_raw.limit(10))

# COMMAND ----------

# Catálogo de productos
df_products_raw = generate_sample_products(spark)
log_dataframe_stats(df_products_raw, "products_raw")
display(df_products_raw)

# COMMAND ----------

# Clientes
df_customers_raw = generate_sample_customers(spark)
log_dataframe_stats(df_customers_raw, "customers_raw")
display(df_customers_raw.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Escritura en la capa Bronze (Delta Lake)
# MAGIC
# MAGIC Los datos se persisten **sin ninguna transformación** para preservar
# MAGIC la fuente original y permitir reprocesamiento.

# COMMAND ----------

# Añadir metadatos de ingesta antes de escribir
from pyspark.sql import functions as F

df_orders_bronze = df_orders_raw.withColumn("_ingestion_timestamp", F.current_timestamp()) \
                                 .withColumn("_source", F.lit("simulated"))

df_products_bronze = df_products_raw.withColumn("_ingestion_timestamp", F.current_timestamp()) \
                                     .withColumn("_source", F.lit("simulated"))

df_customers_bronze = df_customers_raw.withColumn("_ingestion_timestamp", F.current_timestamp()) \
                                       .withColumn("_source", F.lit("simulated"))

# COMMAND ----------

write_delta(
    df_orders_bronze,
    path=f"{BRONZE_PATH}/orders",
    table_name=BRONZE_ORDERS_TABLE,
    mode="overwrite",
    partition_by=["status"],
)
print(f"✅ Tabla Bronze: {BRONZE_ORDERS_TABLE}")

# COMMAND ----------

write_delta(
    df_products_bronze,
    path=f"{BRONZE_PATH}/products",
    table_name=BRONZE_PRODUCTS_TABLE,
    mode="overwrite",
)
print(f"✅ Tabla Bronze: {BRONZE_PRODUCTS_TABLE}")

# COMMAND ----------

write_delta(
    df_customers_bronze,
    path=f"{BRONZE_PATH}/customers",
    table_name=BRONZE_CUSTOMERS_TABLE,
    mode="overwrite",
    partition_by=["country"],
)
print(f"✅ Tabla Bronze: {BRONZE_CUSTOMERS_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Métricas de ingesta

# COMMAND ----------

metrics = {
    "orders":    spark.read.format("delta").load(f"{BRONZE_PATH}/orders").count(),
    "products":  spark.read.format("delta").load(f"{BRONZE_PATH}/products").count(),
    "customers": spark.read.format("delta").load(f"{BRONZE_PATH}/customers").count(),
}

print("📊 Registros ingestados:")
for entity, count in metrics.items():
    print(f"   {entity:<12} → {count:,} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Capa Bronze completada
# MAGIC
# MAGIC Continúa con el notebook **02_transformation_silver** para la limpieza
# MAGIC y normalización de los datos.
