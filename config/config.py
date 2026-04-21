"""
Configuración centralizada del Proyecto Final Databricks.

Contiene todos los parámetros de rutas, esquemas y opciones
usados por los notebooks del pipeline.
"""

# ---------------------------------------------------------------------------
# Entorno
# ---------------------------------------------------------------------------
ENV = "dev"  # Opciones: "dev", "staging", "prod"

# ---------------------------------------------------------------------------
# Rutas base en DBFS / almacenamiento en la nube
# ---------------------------------------------------------------------------
BASE_PATH = f"/FileStore/flujo-databricks/{ENV}"

RAW_PATH = f"{BASE_PATH}/raw"
BRONZE_PATH = f"{BASE_PATH}/bronze"
SILVER_PATH = f"{BASE_PATH}/silver"
GOLD_PATH = f"{BASE_PATH}/gold"
MODELS_PATH = f"{BASE_PATH}/models"
CHECKPOINT_PATH = f"{BASE_PATH}/checkpoints"

# ---------------------------------------------------------------------------
# Nombres de tablas Delta
# ---------------------------------------------------------------------------
DATABASE_NAME = f"flujo_databricks_{ENV}"

BRONZE_ORDERS_TABLE = f"{DATABASE_NAME}.bronze_orders"
BRONZE_PRODUCTS_TABLE = f"{DATABASE_NAME}.bronze_products"
BRONZE_CUSTOMERS_TABLE = f"{DATABASE_NAME}.bronze_customers"

SILVER_ORDERS_TABLE = f"{DATABASE_NAME}.silver_orders"
SILVER_PRODUCTS_TABLE = f"{DATABASE_NAME}.silver_products"
SILVER_CUSTOMERS_TABLE = f"{DATABASE_NAME}.silver_customers"

GOLD_SALES_SUMMARY_TABLE = f"{DATABASE_NAME}.gold_sales_summary"
GOLD_PRODUCT_PERFORMANCE_TABLE = f"{DATABASE_NAME}.gold_product_performance"
GOLD_CUSTOMER_SEGMENTS_TABLE = f"{DATABASE_NAME}.gold_customer_segments"

# ---------------------------------------------------------------------------
# Parámetros de ingesta
# ---------------------------------------------------------------------------
INGEST_OPTIONS = {
    "header": "true",
    "inferSchema": "true",
    "sep": ",",
    "encoding": "UTF-8",
}

# ---------------------------------------------------------------------------
# Parámetros de calidad de datos
# ---------------------------------------------------------------------------
MAX_NULL_RATIO = 0.05          # porcentaje máximo de nulos aceptado por columna
MIN_ORDER_AMOUNT = 0.01        # monto mínimo de una orden válida (USD)
MAX_ORDER_AMOUNT = 1_000_000   # monto máximo de una orden válida (USD)
VALID_STATUS_VALUES = {"pending", "confirmed", "shipped", "delivered", "cancelled"}

# ---------------------------------------------------------------------------
# Parámetros del modelo ML
# ---------------------------------------------------------------------------
ML_EXPERIMENT_NAME = f"/Shared/flujo-databricks/{ENV}/sales_forecast"
ML_MODEL_NAME = f"flujo_databricks_sales_forecast_{ENV}"
ML_TEST_SIZE = 0.2
ML_RANDOM_STATE = 42
ML_FEATURES = [
    "month",
    "day_of_week",
    "product_category_encoded",
    "customer_segment_encoded",
    "unit_price",
    "quantity",
]
ML_TARGET = "total_amount"
