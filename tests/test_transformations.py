"""
Pruebas unitarias para las funciones de transformación del pipeline.

Ejecutar con:
    pytest tests/test_transformations.py -v

Nota: las pruebas usan PySpark en modo local (no requieren un clúster Databricks).
"""

import pytest
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)


# ---------------------------------------------------------------------------
# Fixture: SparkSession local
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def spark():
    """Crea una SparkSession local para las pruebas."""
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("flujo-databricks-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# ---------------------------------------------------------------------------
# Helpers para crear DataFrames de prueba
# ---------------------------------------------------------------------------

def _make_orders(spark, rows):
    schema = StructType([
        StructField("order_id",    StringType(),  True),
        StructField("customer_id", StringType(),  True),
        StructField("product_id",  StringType(),  True),
        StructField("quantity",    IntegerType(), True),
        StructField("unit_price",  DoubleType(),  True),
        StructField("order_date",  StringType(),  True),
        StructField("status",      StringType(),  True),
        StructField("country",     StringType(),  True),
    ])
    return spark.createDataFrame(rows, schema)


# ---------------------------------------------------------------------------
# Pruebas: null_ratio_report
# ---------------------------------------------------------------------------

class TestNullRatioReport:
    def test_no_nulls(self, spark):
        from utils.helpers import null_ratio_report

        df = spark.createDataFrame(
            [("a", 1), ("b", 2)],
            "col1 STRING, col2 INT",
        )
        ratios = null_ratio_report(df)
        assert ratios["col1"] == 0.0
        assert ratios["col2"] == 0.0

    def test_all_nulls(self, spark):
        from utils.helpers import null_ratio_report

        df = spark.createDataFrame(
            [(None, None), (None, None)],
            "col1 STRING, col2 STRING",
        )
        ratios = null_ratio_report(df)
        assert ratios["col1"] == 1.0
        assert ratios["col2"] == 1.0

    def test_partial_nulls(self, spark):
        from utils.helpers import null_ratio_report

        df = spark.createDataFrame(
            [("a",), (None,), (None,), ("d",)],
            "col1 STRING",
        )
        ratios = null_ratio_report(df)
        assert ratios["col1"] == pytest.approx(0.5)

    def test_empty_dataframe(self, spark):
        from utils.helpers import null_ratio_report

        df = spark.createDataFrame([], "col1 STRING, col2 INT")
        ratios = null_ratio_report(df)
        assert ratios["col1"] == 0.0

    def test_subset_columns(self, spark):
        from utils.helpers import null_ratio_report

        df = spark.createDataFrame(
            [(None, 1), (None, 2)],
            "col1 STRING, col2 INT",
        )
        ratios = null_ratio_report(df, columns=["col1"])
        assert "col1" in ratios
        assert "col2" not in ratios


# ---------------------------------------------------------------------------
# Pruebas: remove_duplicates
# ---------------------------------------------------------------------------

class TestRemoveDuplicates:
    def test_no_duplicates(self, spark):
        from utils.helpers import remove_duplicates

        df = spark.createDataFrame([("a",), ("b",), ("c",)], "id STRING")
        result = remove_duplicates(df, subset=["id"])
        assert result.count() == 3

    def test_with_duplicates(self, spark):
        from utils.helpers import remove_duplicates

        df = spark.createDataFrame(
            [("a",), ("a",), ("b",), ("c",), ("c",)], "id STRING"
        )
        result = remove_duplicates(df, subset=["id"])
        assert result.count() == 3

    def test_all_duplicates(self, spark):
        from utils.helpers import remove_duplicates

        df = spark.createDataFrame([("a",), ("a",), ("a",)], "id STRING")
        result = remove_duplicates(df, subset=["id"])
        assert result.count() == 1


# ---------------------------------------------------------------------------
# Pruebas: drop_high_null_columns
# ---------------------------------------------------------------------------

class TestDropHighNullColumns:
    def test_drops_high_null_column(self, spark):
        from utils.helpers import drop_high_null_columns

        df = spark.createDataFrame(
            [(1, None), (2, None), (3, None), (4, None), (5, None)],
            "id INT, bad_col STRING",
        )
        result = drop_high_null_columns(df, max_ratio=0.5)
        assert "bad_col" not in result.columns
        assert "id" in result.columns

    def test_keeps_acceptable_null_column(self, spark):
        from utils.helpers import drop_high_null_columns

        df = spark.createDataFrame(
            [(1, "a"), (2, None), (3, "c")],
            "id INT, col STRING",
        )
        # 1/3 ≈ 0.33 < 0.5 → debe conservarse
        result = drop_high_null_columns(df, max_ratio=0.5)
        assert "col" in result.columns


# ---------------------------------------------------------------------------
# Pruebas: generadores de datos de muestra
# ---------------------------------------------------------------------------

class TestSampleDataGenerators:
    def test_generate_orders_count(self, spark):
        from utils.helpers import generate_sample_orders

        df = generate_sample_orders(spark, n=100)
        assert df.count() == 100

    def test_generate_orders_schema(self, spark):
        from utils.helpers import generate_sample_orders

        df = generate_sample_orders(spark, n=10)
        expected_cols = {
            "order_id", "customer_id", "product_id",
            "quantity", "unit_price", "order_date", "status", "country",
        }
        assert expected_cols.issubset(set(df.columns))

    def test_generate_products_count(self, spark):
        from utils.helpers import generate_sample_products

        df = generate_sample_products(spark)
        assert df.count() == 50

    def test_generate_customers_count(self, spark):
        from utils.helpers import generate_sample_customers

        df = generate_sample_customers(spark)
        assert df.count() == 200

    def test_generate_customers_segments(self, spark):
        from utils.helpers import generate_sample_customers

        df = generate_sample_customers(spark)
        segments = {row["segment"] for row in df.select("segment").distinct().collect()}
        assert segments == {"Bronze", "Silver", "Gold", "Platinum"}


# ---------------------------------------------------------------------------
# Pruebas: lógica de transformación (columnas derivadas)
# ---------------------------------------------------------------------------

class TestTransformationLogic:
    def test_total_amount_calculation(self, spark):
        """total_amount debe ser quantity * unit_price redondeado a 2 decimales."""
        rows = [
            ("ORD-001", "CUST-001", "PROD-001", 3, 10.50,
             "2024-03-15", "delivered", "MX"),
        ]
        df = _make_orders(spark, rows)
        df = df.withColumn(
            "total_amount",
            F.round(F.col("quantity") * F.col("unit_price"), 2),
        )
        result = df.select("total_amount").first()["total_amount"]
        assert result == pytest.approx(31.50)

    def test_status_normalization(self, spark):
        """El estado debe normalizarse a minúsculas y sin espacios."""
        rows = [
            ("ORD-001", "CUST-001", "PROD-001", 1, 5.0,
             "2024-01-01", "  DELIVERED  ", "MX"),
        ]
        df = _make_orders(spark, rows)
        df = df.withColumn("status", F.lower(F.trim(F.col("status"))))
        result = df.select("status").first()["status"]
        assert result == "delivered"

    def test_country_normalization(self, spark):
        """El país debe normalizarse a mayúsculas sin espacios."""
        rows = [
            ("ORD-001", "CUST-001", "PROD-001", 1, 5.0,
             "2024-01-01", "delivered", " mx "),
        ]
        df = _make_orders(spark, rows)
        df = df.withColumn("country", F.upper(F.trim(F.col("country"))))
        result = df.select("country").first()["country"]
        assert result == "MX"

    def test_date_parsing(self, spark):
        """order_date debe parsearse correctamente a DateType."""
        rows = [
            ("ORD-001", "CUST-001", "PROD-001", 1, 5.0,
             "2024-06-15", "delivered", "MX"),
        ]
        df = _make_orders(spark, rows)
        df = df.withColumn("order_date", F.to_date("order_date", "yyyy-MM-dd"))
        result = df.select("order_date").first()["order_date"]
        assert result == date(2024, 6, 15)

    def test_year_month_extraction(self, spark):
        """year y month deben extraerse correctamente de order_date."""
        rows = [
            ("ORD-001", "CUST-001", "PROD-001", 1, 5.0,
             "2024-09-22", "delivered", "MX"),
        ]
        df = _make_orders(spark, rows)
        df = (
            df.withColumn("order_date", F.to_date("order_date", "yyyy-MM-dd"))
              .withColumn("year",  F.year("order_date"))
              .withColumn("month", F.month("order_date"))
        )
        row = df.select("year", "month").first()
        assert row["year"] == 2024
        assert row["month"] == 9

    def test_cancelled_order_flag(self, spark):
        """is_cancelled debe ser True solo para órdenes con status 'cancelled'."""
        rows = [
            ("ORD-001", "C-001", "P-001", 1, 5.0, "2024-01-01", "cancelled", "MX"),
            ("ORD-002", "C-002", "P-002", 1, 5.0, "2024-01-02", "delivered", "MX"),
        ]
        df = _make_orders(spark, rows)
        df = df.withColumn(
            "is_cancelled", (F.col("status") == "cancelled").cast("boolean")
        )
        results = {
            row["order_id"]: row["is_cancelled"]
            for row in df.select("order_id", "is_cancelled").collect()
        }
        assert results["ORD-001"] is True
        assert results["ORD-002"] is False

    def test_filter_cancelled_orders_excluded_from_revenue(self, spark):
        """Las órdenes canceladas no deben sumarse en los ingresos."""
        rows = [
            ("ORD-001", "C-001", "P-001", 2, 100.0, "2024-01-01", "delivered", "MX"),
            ("ORD-002", "C-002", "P-002", 1, 200.0, "2024-01-02", "cancelled", "MX"),
        ]
        df = _make_orders(spark, rows)
        df = df.withColumn("total_amount", F.col("quantity") * F.col("unit_price"))
        revenue = (
            df.filter(F.col("status") != "cancelled")
              .agg(F.sum("total_amount"))
              .first()[0]
        )
        assert revenue == pytest.approx(200.0)
