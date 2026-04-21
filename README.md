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

## Despliegue en Azure (cuenta nueva)

> Sigue estos pasos si acabas de crear tu cuenta en [portal.azure.com](https://portal.azure.com).  
> Tienes **200 USD de créditos** gratuitos — más que suficiente para ejecutar este proyecto.

### Opción A — Un clic desde GitHub Actions (recomendado)

#### Paso 1 — Instalar Azure CLI y crear el Service Principal

En tu computadora (Windows PowerShell o terminal de macOS/Linux):

```bash
# 1. Instalar Azure CLI si aún no lo tienes
# Windows: https://aka.ms/installazurecli
# macOS: brew install azure-cli
# Linux: curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash

# 2. Iniciar sesión
az login

# 3. Ver tu suscripción activa y copiar el "id"
az account show --query "{id:id, name:name}" -o table

# 4. Crear el Service Principal (reemplaza <ID_SUSCRIPCION> con el valor del paso anterior)
az ad sp create-for-rbac \
  --name "flujo-databricks-sp" \
  --role contributor \
  --scopes /subscriptions/<ID_SUSCRIPCION> \
  --sdk-auth
```

Ese último comando imprime un JSON como este — **cópialo completo**:

```json
{
  "clientId": "...",
  "clientSecret": "...",
  "subscriptionId": "...",
  "tenantId": "...",
  ...
}
```

#### Paso 2 — Agregar el secreto en GitHub

1. Ve a tu repositorio en GitHub → **Settings → Secrets and variables → Actions**
2. Haz clic en **New repository secret**
3. Nombre: `AZURE_CREDENTIALS`
4. Valor: pega el JSON completo del paso anterior
5. Haz clic en **Add secret**

#### Paso 3 — Ejecutar el workflow

1. Ve a la pestaña **Actions** de tu repositorio en GitHub
2. Selecciona el workflow **"🚀 Desplegar en Azure Databricks"**
3. Haz clic en **Run workflow**
4. Elige el entorno (`dev`) y la región (`eastus2`) → **Run workflow**
5. Espera ~10 minutos hasta que finalice ✅

Al terminar verás en el resumen del job la **URL de tu workspace de Databricks**.

---

### Opción B — Script local (bash)

```bash
# 1. Clona el repositorio (si aún no lo tienes)
git clone https://github.com/joespinch/flujo-databricks.git
cd flujo-databricks

# 2. Inicia sesión en Azure
az login

# 3. Ejecuta el script de despliegue
chmod +x deploy/deploy.sh
./deploy/deploy.sh
```

El script crea automáticamente:
- El **grupo de recursos** `flujo-databricks-rg`
- El **workspace de Azure Databricks**
- El **Storage Account** (ADLS Gen2) para los datos del pipeline
- Un **clúster de Spark** single-node con apagado automático a los 30 minutos
- El **repo clonado** en Databricks Repos

---

## Cómo ejecutar el pipeline una vez desplegado

1. Abre la URL del workspace que aparece al final del despliegue
2. Ve a **Repos → flujo-databricks → notebooks**
3. Abre cada notebook y adjúntalo al clúster `flujo-databricks-dev`
4. Ejecuta los notebooks en orden: `01 → 02 → 03 → 04`
5. Las pruebas unitarias se ejecutan con `pytest tests/`

```
01_ingestion_bronze.py   →   ingesta datos CSV → capa Bronze (Delta Lake)
02_transformation_silver →   limpieza y validación → capa Silver
03_aggregation_gold.py   →   KPIs y métricas → capa Gold
04_ml_model.py           →   modelo ML de predicción de ventas con MLflow
```

## Archivos de despliegue

```
deploy/
├── main.bicep            # Plantilla de infraestructura Azure (Databricks + Storage)
├── parameters.json       # Parámetros: región, entorno, tier
├── deploy.sh             # Script bash todo-en-uno
└── setup_databricks.py   # Configura el workspace (clúster, repos, MLflow)

.github/workflows/
└── deploy-azure.yml      # GitHub Actions: despliegue con un clic
```

## Dataset

El proyecto utiliza datos simulados de ventas con las siguientes entidades:
- **Órdenes**: transacciones de compra con fecha, cliente y monto
- **Productos**: catálogo con categoría y precio unitario
- **Clientes**: información demográfica y segmento de cliente
