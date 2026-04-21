# Flujo Databricks — Proyecto Final

Implementación de un pipeline de datos completo con **Arquitectura Medallón** (Bronze → Silver → Gold) usando Apache Spark y Delta Lake en Databricks.

## Descripción

Este proyecto simula un flujo de datos de ventas de una empresa de comercio electrónico. A través de tres capas de transformación se obtienen métricas de negocio listas para análisis y toma de decisiones. Además incluye un módulo de Machine Learning para predicción de ventas.

## Arquitectura

```
Datos Crudos (CSV/JSON)
        │
        ▼
┌───────────────┐
│  Bronze Layer  │  Ingesta y almacenamiento raw en Delta Lake
│  (01_ingestion)│
└───────┬───────┘
        │
        ▼
┌───────────────┐
│  Silver Layer  │  Limpieza, validación y transformación
│  (02_transform)│
└───────┬───────┘
        │
        ▼
┌───────────────┐
│   Gold Layer   │  Agregaciones de negocio y KPIs
│  (03_aggregat) │
└───────┬───────┘
        │
        ▼
┌───────────────┐
│  ML Model      │  Predicción de ventas con MLflow
│  (04_ml_model) │
└───────────────┘
```

## Estructura del Proyecto

```
flujo-databricks/
├── README.md
├── notebooks/
│   ├── 01_ingestion_bronze.py      # Ingesta de datos crudos → Bronze
│   ├── 02_transformation_silver.py  # Limpieza y transformación → Silver
│   ├── 03_aggregation_gold.py       # Agregaciones de negocio → Gold
│   └── 04_ml_model.py              # Modelo ML con MLflow
├── config/
│   └── config.py                   # Parámetros centralizados del proyecto
├── utils/
│   └── helpers.py                  # Funciones auxiliares reutilizables
└── tests/
    └── test_transformations.py     # Pruebas unitarias de las transformaciones
```

## Capas de Datos

| Capa   | Descripción                                      | Formato   |
|--------|--------------------------------------------------|-----------|
| Bronze | Datos crudos ingestados sin modificar            | Delta Lake |
| Silver | Datos limpios, validados y normalizados          | Delta Lake |
| Gold   | Métricas y KPIs listos para consumo analítico   | Delta Lake |

## Tecnologías

- **Apache Spark** — procesamiento distribuido de datos
- **Delta Lake** — almacenamiento transaccional ACID
- **MLflow** — seguimiento de experimentos y registro de modelos
- **Databricks** — plataforma unificada de datos e IA

## Cómo Ejecutar

1. Importar los notebooks al workspace de Databricks en el orden numérico.
2. Configurar los parámetros en `config/config.py` según el entorno.
3. Ejecutar los notebooks de forma secuencial: `01 → 02 → 03 → 04`.
4. Las pruebas unitarias se ejecutan con `pytest tests/`.

## Dataset

El proyecto utiliza datos simulados de ventas con las siguientes entidades:
- **Órdenes**: transacciones de compra con fecha, cliente y monto
- **Productos**: catálogo con categoría y precio unitario
- **Clientes**: información demográfica y segmento de cliente
