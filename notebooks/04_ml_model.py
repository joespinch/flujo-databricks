# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Modelo de Machine Learning (Predicción de Ventas)
# MAGIC
# MAGIC **Objetivo:** entrenar un modelo de regresión para predecir el monto
# MAGIC de una orden a partir de sus características, usando MLflow para el
# MAGIC seguimiento de experimentos y el registro del modelo.
# MAGIC
# MAGIC | Paso | Acción |
# MAGIC |------|--------|
# MAGIC | 1    | Preparar dataset de entrenamiento desde Silver |
# MAGIC | 2    | Feature engineering |
# MAGIC | 3    | Dividir en train / test |
# MAGIC | 4    | Entrenar y evaluar el modelo |
# MAGIC | 5    | Registrar experimento y modelo en MLflow |
# MAGIC | 6    | Cargar el modelo y hacer predicciones de ejemplo |

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/flujo-databricks")

import mlflow
import mlflow.spark
import mlflow.sklearn

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import functions as F

from config.config import (
    SILVER_PATH,
    ML_EXPERIMENT_NAME,
    ML_MODEL_NAME,
    ML_TEST_SIZE,
    ML_RANDOM_STATE,
    ML_FEATURES,
    ML_TARGET,
)
from utils.helpers import log_info

print("✅ Módulos importados correctamente")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Preparar dataset de entrenamiento

# COMMAND ----------

df_orders = spark.read.format("delta").load(f"{SILVER_PATH}/orders")

# Usar sólo órdenes no canceladas con datos completos
df_ml = (
    df_orders
    .filter(~F.col("is_cancelled"))
    .filter(F.col("category").isNotNull())
    .filter(F.col("segment").isNotNull())
    .select(
        "month",
        "day_of_week",
        "category",
        "segment",
        "unit_price",
        "quantity",
        ML_TARGET,
    )
    .dropna()
)

print(f"📊 Dataset de ML: {df_ml.count():,} filas")
display(df_ml.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Feature Engineering

# COMMAND ----------

# Codificar variables categóricas con StringIndexer
category_indexer = StringIndexer(
    inputCol="category",
    outputCol="product_category_encoded",
    handleInvalid="keep",
)
segment_indexer = StringIndexer(
    inputCol="segment",
    outputCol="customer_segment_encoded",
    handleInvalid="keep",
)

# Vector de características
feature_cols = [
    "month",
    "day_of_week",
    "product_category_encoded",
    "customer_segment_encoded",
    "unit_price",
    "quantity",
]

assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol="features_raw",
)

scaler = StandardScaler(
    inputCol="features_raw",
    outputCol="features",
    withMean=True,
    withStd=True,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. División train / test

# COMMAND ----------

df_train, df_test = df_ml.randomSplit(
    [1 - ML_TEST_SIZE, ML_TEST_SIZE],
    seed=ML_RANDOM_STATE,
)

print(f"🏋️  Train: {df_train.count():,} filas")
print(f"🧪 Test:  {df_test.count():,} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Entrenamiento del modelo (Gradient Boosted Trees)

# COMMAND ----------

gbt = GBTRegressor(
    featuresCol="features",
    labelCol=ML_TARGET,
    maxIter=50,
    maxDepth=5,
    stepSize=0.1,
    seed=ML_RANDOM_STATE,
)

pipeline = Pipeline(stages=[
    category_indexer,
    segment_indexer,
    assembler,
    scaler,
    gbt,
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Registro de experimento en MLflow

# COMMAND ----------

mlflow.set_experiment(ML_EXPERIMENT_NAME)

with mlflow.start_run(run_name="GBT_sales_forecast") as run:

    # Hiperparámetros
    mlflow.log_params({
        "model_type":   "GradientBoostedTrees",
        "max_iter":     gbt.getMaxIter(),
        "max_depth":    gbt.getMaxDepth(),
        "step_size":    gbt.getStepSize(),
        "test_size":    ML_TEST_SIZE,
        "random_state": ML_RANDOM_STATE,
        "train_rows":   df_train.count(),
        "test_rows":    df_test.count(),
    })

    # Entrenar
    log_info("Iniciando entrenamiento del modelo GBT...")
    model = pipeline.fit(df_train)

    # Evaluar
    evaluator_rmse = RegressionEvaluator(
        labelCol=ML_TARGET, predictionCol="prediction", metricName="rmse"
    )
    evaluator_r2 = RegressionEvaluator(
        labelCol=ML_TARGET, predictionCol="prediction", metricName="r2"
    )
    evaluator_mae = RegressionEvaluator(
        labelCol=ML_TARGET, predictionCol="prediction", metricName="mae"
    )

    df_predictions = model.transform(df_test)

    rmse = evaluator_rmse.evaluate(df_predictions)
    r2   = evaluator_r2.evaluate(df_predictions)
    mae  = evaluator_mae.evaluate(df_predictions)

    # Métricas
    mlflow.log_metrics({"rmse": rmse, "r2": r2, "mae": mae})

    print("=" * 45)
    print("       📈 MÉTRICAS DE EVALUACIÓN")
    print("=" * 45)
    print(f"  RMSE : {rmse:>10.4f}")
    print(f"  MAE  : {mae:>10.4f}")
    print(f"  R²   : {r2:>10.4f}")
    print("=" * 45)

    # Registrar modelo en MLflow Model Registry
    mlflow.spark.log_model(
        spark_model=model,
        artifact_path="model",
        registered_model_name=ML_MODEL_NAME,
    )

    run_id = run.info.run_id
    print(f"\n✅ Experimento registrado: {ML_EXPERIMENT_NAME}")
    print(f"   Run ID: {run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Predicciones de ejemplo

# COMMAND ----------

df_sample = df_test.limit(10)
df_sample_preds = model.transform(df_sample)

display(
    df_sample_preds.select(
        ML_TARGET,
        "prediction",
        F.round(
            (F.abs(F.col("prediction") - F.col(ML_TARGET)) / F.col(ML_TARGET)) * 100,
            2,
        ).alias("pct_error"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Modelo ML completado
# MAGIC
# MAGIC El modelo ha sido entrenado, evaluado y registrado en MLflow.
# MAGIC Consulta el **Model Registry** en la barra lateral de Databricks
# MAGIC para gestionar versiones y desplegar el modelo a producción.
