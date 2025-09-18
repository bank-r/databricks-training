$loc = "southeastasia"
$rgDev = "rg-medisure-dev"
$saDev = "samedisuredev"
$dacDev = "dac-medisure-dev" 

# --- Create the Access Connector (system-assigned managed identity) ---
az databricks access-connector create `
    --resource-group $rgDev `
    --name $dacDev `
    --location $loc `
    --identity-type SystemAssigned

# --- Get IDs we need ---
#$acId = az databricks access-connector show -g $rgDev -n $dacDev --query id -o tsv
$principalId = az databricks access-connector show -g $rgDev -n $dacDev --query identity.principalId -o tsv
$saId = az storage account show -n $saDev -g $rgDev --query id -o tsv

# --- Grant RBAC to the Access Connector on the storage account ---
az role assignment create `
    --assignee-object-id $principalId `
    --assignee-principal-type ServicePrincipal `
    --role "Storage Blob Data Contributor" `
    --scope $saId

# --- Ensure the folders exist for a managed catalog root (optional but recommended) ---
# We'll put the catalog's managed location under the container medisure
az storage fs directory create --account-name $saDev --file-system medisure --name managed-delta-files --auth-mode login | Out-Null
