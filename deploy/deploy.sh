#!/usr/bin/env bash
# =============================================================================
# deploy.sh — Despliega la infraestructura de Azure para Flujo Databricks
#
# Requisitos:
#   - Azure CLI instalado  (https://docs.microsoft.com/cli/azure/install-azure-cli)
#   - Sesión activa con: az login
#
# Uso:
#   chmod +x deploy/deploy.sh
#   ./deploy/deploy.sh
# =============================================================================
set -euo pipefail

# ---------------------------------------------------------------------------
# Configuración — edita estos valores si lo deseas
# ---------------------------------------------------------------------------
RESOURCE_GROUP="flujo-databricks-rg"
LOCATION="eastus2"                   # Región con buena disponibilidad de Databricks
DEPLOYMENT_NAME="flujo-databricks-$(date +%Y%m%d%H%M%S)"
PARAMETERS_FILE="deploy/parameters.json"
TEMPLATE_FILE="deploy/main.bicep"

# ---------------------------------------------------------------------------
# Colores para la salida
# ---------------------------------------------------------------------------
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

log_info()    { echo -e "${GREEN}[INFO]${NC}  $1"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC}  $1"; }
log_error()   { echo -e "${RED}[ERROR]${NC} $1"; }

# ---------------------------------------------------------------------------
# 1. Verificar que Azure CLI está instalado y autenticado
# ---------------------------------------------------------------------------
log_info "Verificando Azure CLI..."
if ! command -v az &>/dev/null; then
  log_error "Azure CLI no encontrado. Instálalo desde: https://aka.ms/installazurecli"
  exit 1
fi

ACCOUNT=$(az account show --query "user.name" -o tsv 2>/dev/null || true)
if [[ -z "$ACCOUNT" ]]; then
  log_warn "No hay sesión activa. Iniciando sesión en Azure..."
  az login
fi
log_info "Sesión activa como: $(az account show --query 'user.name' -o tsv)"
log_info "Suscripción: $(az account show --query 'name' -o tsv)"

# ---------------------------------------------------------------------------
# 2. Crear el grupo de recursos
# ---------------------------------------------------------------------------
log_info "Creando grupo de recursos: $RESOURCE_GROUP en $LOCATION..."
az group create \
  --name "$RESOURCE_GROUP" \
  --location "$LOCATION" \
  --tags project=flujo-databricks \
  --output none
log_info "Grupo de recursos listo: $RESOURCE_GROUP"

# ---------------------------------------------------------------------------
# 3. Desplegar la plantilla Bicep
# ---------------------------------------------------------------------------
log_info "Desplegando infraestructura con Bicep (puede tardar 5-10 minutos)..."
DEPLOYMENT_OUTPUT=$(az deployment group create \
  --name "$DEPLOYMENT_NAME" \
  --resource-group "$RESOURCE_GROUP" \
  --template-file "$TEMPLATE_FILE" \
  --parameters "@$PARAMETERS_FILE" \
  --output json)

# Extraer outputs
WORKSPACE_URL=$(echo "$DEPLOYMENT_OUTPUT" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['properties']['outputs']['databricksWorkspaceUrl']['value'])")
WORKSPACE_NAME=$(echo "$DEPLOYMENT_OUTPUT" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['properties']['outputs']['databricksWorkspaceName']['value'])")
STORAGE_NAME=$(echo "$DEPLOYMENT_OUTPUT" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['properties']['outputs']['storageAccountName']['value'])")

# ---------------------------------------------------------------------------
# 4. Generar Personal Access Token de Databricks
# ---------------------------------------------------------------------------
log_info "Generando token de acceso para Databricks..."

# Obtener el token de Entra ID para la API de Databricks
MANAGEMENT_TOKEN=$(az account get-access-token \
  --resource "https://management.core.windows.net/" \
  --query "accessToken" -o tsv)
DATABRICKS_TOKEN=$(az account get-access-token \
  --resource "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d" \
  --query "accessToken" -o tsv)

SUBSCRIPTION_ID=$(az account show --query "id" -o tsv)
RESOURCE_ID=$(az databricks workspace show \
  --name "$WORKSPACE_NAME" \
  --resource-group "$RESOURCE_GROUP" \
  --query "id" -o tsv)

# Crear PAT usando la API de Databricks
PAT_RESPONSE=$(curl -s -X POST \
  "${WORKSPACE_URL}/api/2.0/token/create" \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  -H "X-Databricks-Azure-SP-Management-Token: $MANAGEMENT_TOKEN" \
  -H "X-Databricks-Azure-Workspace-Resource-Id: $RESOURCE_ID" \
  -H "Content-Type: application/json" \
  -d '{"comment": "flujo-databricks-deploy", "lifetime_seconds": 7776000}')

PAT=$(echo "$PAT_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('token_value',''))" 2>/dev/null || true)

# ---------------------------------------------------------------------------
# 5. Configurar Databricks (clúster + repos + MLflow)
# ---------------------------------------------------------------------------
if [[ -n "$PAT" ]]; then
  log_info "Configurando el workspace de Databricks..."
  pip install --quiet databricks-sdk
  DATABRICKS_HOST="$WORKSPACE_URL" DATABRICKS_TOKEN="$PAT" python3 deploy/setup_databricks.py
else
  log_warn "No se pudo generar el PAT automáticamente."
  log_warn "Configura Databricks manualmente — ver README.md Paso 4."
fi

# ---------------------------------------------------------------------------
# 6. Guardar credenciales en archivo local (NO se sube al repo)
# ---------------------------------------------------------------------------
CREDS_FILE=".databricks_credentials"
cat > "$CREDS_FILE" <<EOF
# Generado automáticamente por deploy.sh — NO subir a Git
DATABRICKS_HOST=$WORKSPACE_URL
DATABRICKS_TOKEN=$PAT
STORAGE_ACCOUNT=$STORAGE_NAME
RESOURCE_GROUP=$RESOURCE_GROUP
EOF
log_warn "Credenciales guardadas en $CREDS_FILE (excluido del repositorio)"

# ---------------------------------------------------------------------------
# 7. Resumen final
# ---------------------------------------------------------------------------
echo ""
echo "================================================================"
echo "   ✅  DESPLIEGUE COMPLETADO EXITOSAMENTE"
echo "================================================================"
echo "   Workspace de Databricks:"
echo "   → $WORKSPACE_URL"
echo ""
echo "   Storage Account: $STORAGE_NAME"
echo "   Resource Group : $RESOURCE_GROUP"
echo "================================================================"
echo ""
echo "📋 Próximos pasos:"
echo "   1. Abre el workspace: $WORKSPACE_URL"
echo "   2. Ve a Repos → flujo-databricks → notebooks"
echo "   3. Ejecuta los notebooks en orden: 01 → 02 → 03 → 04"
echo "   4. Adjunta cada notebook al clúster: flujo-databricks-dev"
echo ""
