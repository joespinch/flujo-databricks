// =============================================================================
// Flujo Databricks — Plantilla de Infraestructura en Azure (Bicep)
//
// Recursos que crea esta plantilla:
//   1. Azure Databricks Workspace (Premium tier — Standard está deprecado)
//   2. Storage Account con ADLS Gen2 para los datos del pipeline
//   3. Contenedor "flujo-databricks" dentro del storage account
// =============================================================================

@description('Prefijo para nombrar todos los recursos (ej. "flujodb")')
@minLength(3)
@maxLength(10)
param projectPrefix string = 'flujodb'

@description('Entorno de despliegue')
@allowed(['dev', 'staging', 'prod'])
param environment string = 'dev'

@description('Región de Azure donde se crearán los recursos')
param location string = resourceGroup().location

@description('Tier de precios de Databricks: premium (Standard está deprecado)')
@allowed(['standard', 'premium'])
param databricksTier string = 'premium'

// ---------------------------------------------------------------------------
// Variables
// ---------------------------------------------------------------------------
var suffix = uniqueString(resourceGroup().id)
var workspaceName = '${projectPrefix}-databricks-${environment}-${take(suffix, 6)}'
var storageAccountName = '${projectPrefix}adls${environment}${take(suffix, 6)}'
var managedResourceGroupName = 'databricks-rg-${workspaceName}'

// ---------------------------------------------------------------------------
// 1. Azure Databricks Workspace
// ---------------------------------------------------------------------------
resource databricksWorkspace 'Microsoft.Databricks/workspaces@2023-02-01' = {
  name: workspaceName
  location: location
  sku: {
    name: databricksTier
  }
  properties: {
    managedResourceGroupId: subscriptionResourceId(
      'Microsoft.Resources/resourceGroups',
      managedResourceGroupName
    )
    parameters: {
      enableNoPublicIp: {
        value: false
      }
    }
  }
  tags: {
    project: 'flujo-databricks'
    environment: environment
    createdBy: 'bicep-deployment'
  }
}

// ---------------------------------------------------------------------------
// 2. Storage Account (ADLS Gen2)
// ---------------------------------------------------------------------------
resource storageAccount 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: storageAccountName
  location: location
  sku: {
    name: 'Standard_LRS'   // LRS = más económico, suficiente para dev
  }
  kind: 'StorageV2'
  properties: {
    isHnsEnabled: true      // Activa ADLS Gen2 (Hierarchical Namespace)
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
    supportsHttpsTrafficOnly: true
  }
  tags: {
    project: 'flujo-databricks'
    environment: environment
  }
}

// ---------------------------------------------------------------------------
// 3. Contenedor de datos en el Storage Account
// ---------------------------------------------------------------------------
resource blobService 'Microsoft.Storage/storageAccounts/blobServices@2023-01-01' = {
  parent: storageAccount
  name: 'default'
}

resource dataContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  parent: blobService
  name: 'flujo-databricks'
  properties: {
    publicAccess: 'None'
  }
}

// ---------------------------------------------------------------------------
// Outputs — valores necesarios para configurar Databricks
// ---------------------------------------------------------------------------
output databricksWorkspaceName string = databricksWorkspace.name
output databricksWorkspaceUrl string = 'https://${databricksWorkspace.properties.workspaceUrl}'
output databricksWorkspaceId string = databricksWorkspace.id
output storageAccountName string = storageAccount.name
output storageAccountId string = storageAccount.id
output dataContainerName string = dataContainer.name
output resourceGroupName string = resourceGroup().name
output location string = location
