"""
Funciones auxiliares reutilizables para el pipeline de datos.

Estas utilidades son usadas por los notebooks del proyecto para operaciones
comunes: logging, validación de esquemas, escritura en Delta Lake, etc.
"""

from __future__ import annotations

import logging
from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("flujo_databricks")


# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------

def log_info(message: str) -> None:
    """Log an informational message."""
    logger.info(message)


def log_dataframe_stats(df: DataFrame, name: str) -> None:
    """Print row count and schema for a DataFrame."""
    count = df.count()
    log_info(f"[{name}] filas={count:,}, columnas={len(df.columns)}")
    df.printSchema()


# ---------------------------------------------------------------------------
# Validación de esquema
# ---------------------------------------------------------------------------

def validate_schema(df: DataFrame, expected_schema: StructType) -> bool:
    """
    Verifica que el DataFrame tenga al menos las columnas del esquema esperado.

    Parameters
    ----------
    df : DataFrame
        DataFrame a validar.
    expected_schema : StructType
        Esquema con las columnas y tipos esperados.

    Returns
    -------
    bool
        True si el esquema es compatible, False en caso contrario.
    """
    actual_fields = {f.name: f.dataType for f in df.schema.fields}
    for field in expected_schema.fields:
        if field.name not in actual_fields:
            log_info(f"Columna faltante: {field.name}")
            return False
        if not isinstance(actual_fields[field.name], type(field.dataType)):
            log_info(
                f"Tipo incorrecto para '{field.name}': "
                f"esperado {field.dataType}, encontrado {actual_fields[field.name]}"
            )
            return False
    return True


# ---------------------------------------------------------------------------
# Control de calidad de datos
# ---------------------------------------------------------------------------

def null_ratio_report(df: DataFrame, columns: List[str] | None = None) -> dict:
    """
    Calcula el porcentaje de nulos por columna.

    Parameters
    ----------
    df : DataFrame
        DataFrame a analizar.
    columns : list of str, optional
        Subconjunto de columnas a evaluar. Por defecto todas.

    Returns
    -------
    dict
        Diccionario {columna: ratio_nulos}.
    """
    cols = columns or df.columns
    total = df.count()
    if total == 0:
        return {c: 0.0 for c in cols}
    ratios = {}
    for col in cols:
        null_count = df.filter(F.col(col).isNull()).count()
        ratios[col] = null_count / total
    return ratios


def drop_high_null_columns(df: DataFrame, max_ratio: float = 0.05) -> DataFrame:
    """
    Elimina columnas cuyo ratio de nulos supera el umbral indicado.

    Parameters
    ----------
    df : DataFrame
    max_ratio : float
        Umbral máximo de nulos (0–1).

    Returns
    -------
    DataFrame con las columnas de baja calidad eliminadas.
    """
    ratios = null_ratio_report(df)
    cols_to_drop = [col for col, ratio in ratios.items() if ratio > max_ratio]
    if cols_to_drop:
        log_info(f"Columnas eliminadas por exceso de nulos: {cols_to_drop}")
    return df.drop(*cols_to_drop)


def remove_duplicates(df: DataFrame, subset: List[str] | None = None) -> DataFrame:
    """
    Elimina filas duplicadas del DataFrame.

    Parameters
    ----------
    df : DataFrame
    subset : list of str, optional
        Columnas a considerar para detectar duplicados.

    Returns
    -------
    DataFrame deduplicado.
    """
    before = df.count()
    df_dedup = df.dropDuplicates(subset)
    after = df_dedup.count()
    log_info(f"Duplicados eliminados: {before - after:,}")
    return df_dedup


# ---------------------------------------------------------------------------
# Escritura en Delta Lake
# ---------------------------------------------------------------------------

def write_delta(
    df: DataFrame,
    path: str,
    table_name: str,
    mode: str = "overwrite",
    partition_by: List[str] | None = None,
) -> None:
    """
    Escribe un DataFrame como tabla Delta Lake.

    Parameters
    ----------
    df : DataFrame
    path : str
        Ruta en DBFS/almacenamiento donde se guardará la tabla.
    table_name : str
        Nombre completo de la tabla (base_datos.nombre_tabla).
    mode : str
        Modo de escritura: "overwrite" o "append".
    partition_by : list of str, optional
        Columnas para particionar la tabla.
    """
    writer = df.write.format("delta").mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)

    # Registrar la tabla en el metastore de Spark
    spark = SparkSession.getActiveSession()
    if spark is not None:
        spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name} USING DELTA LOCATION '{path}'")
    log_info(f"Tabla '{table_name}' escrita en '{path}' (modo={mode})")


# ---------------------------------------------------------------------------
# Generación de datos simulados
# ---------------------------------------------------------------------------

def generate_sample_orders(spark: SparkSession, n: int = 1000) -> DataFrame:
    """
    Genera un DataFrame con órdenes de venta simuladas para desarrollo.

    Parameters
    ----------
    spark : SparkSession
    n : int
        Número de órdenes a generar.

    Returns
    -------
    DataFrame con columnas:
        order_id, customer_id, product_id, quantity, unit_price,
        order_date, status, country
    """
    import random
    from datetime import date, timedelta

    random.seed(42)
    statuses = ["pending", "confirmed", "shipped", "delivered", "cancelled"]
    countries = ["MX", "CO", "AR", "CL", "PE", "EC", "BO"]

    rows = []
    base_date = date(2024, 1, 1)
    for i in range(1, n + 1):
        order_date = base_date + timedelta(days=random.randint(0, 364))
        quantity = random.randint(1, 20)
        unit_price = round(random.uniform(5.0, 500.0), 2)
        rows.append(
            (
                f"ORD-{i:06d}",
                f"CUST-{random.randint(1, 200):04d}",
                f"PROD-{random.randint(1, 50):04d}",
                quantity,
                unit_price,
                str(order_date),
                random.choice(statuses),
                random.choice(countries),
            )
        )

    schema = (
        "order_id STRING, customer_id STRING, product_id STRING, "
        "quantity INT, unit_price DOUBLE, order_date STRING, "
        "status STRING, country STRING"
    )
    return spark.createDataFrame(rows, schema)


def generate_sample_products(spark: SparkSession) -> DataFrame:
    """
    Genera un DataFrame con el catálogo de productos simulado.

    Returns
    -------
    DataFrame con columnas: product_id, product_name, category, unit_price
    """
    categories = ["Electrónica", "Ropa", "Hogar", "Deportes", "Libros", "Juguetes"]
    rows = []
    for i in range(1, 51):
        category = categories[(i - 1) % len(categories)]
        rows.append(
            (
                f"PROD-{i:04d}",
                f"Producto {i}",
                category,
                round(5.0 + i * 9.5, 2),
            )
        )
    schema = "product_id STRING, product_name STRING, category STRING, unit_price DOUBLE"
    return spark.createDataFrame(rows, schema)


def generate_sample_customers(spark: SparkSession) -> DataFrame:
    """
    Genera un DataFrame con datos de clientes simulados.

    Returns
    -------
    DataFrame con columnas: customer_id, name, segment, country, registration_date
    """
    segments = ["Bronze", "Silver", "Gold", "Platinum"]
    countries = ["MX", "CO", "AR", "CL", "PE", "EC", "BO"]
    rows = []
    from datetime import date, timedelta
    import random

    random.seed(42)
    base = date(2020, 1, 1)
    for i in range(1, 201):
        reg = base + timedelta(days=random.randint(0, 1460))
        rows.append(
            (
                f"CUST-{i:04d}",
                f"Cliente {i}",
                segments[i % len(segments)],
                countries[i % len(countries)],
                str(reg),
            )
        )
    schema = (
        "customer_id STRING, name STRING, segment STRING, "
        "country STRING, registration_date STRING"
    )
    return spark.createDataFrame(rows, schema)
