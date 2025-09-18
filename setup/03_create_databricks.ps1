# --- 0) (If needed) ensure you're logged in with ARM scope + pick subscription ---
# az login --use-device-code --tenant <YOUR_TENANT> --scope https://management.core.windows.net//.default
# $sub = Read-Host "Enter Subscription ID or Name"; az account set --subscription $sub

# --- 1) Install extension & register the resource provider ---
az extension add -n databricks --upgrade
az provider register --namespace Microsoft.Databricks
az provider wait --namespace Microsoft.Databricks --state Registered

# --- 2) Vars (adjust as needed) ---
$loc = "southeastasia"
$rgDev = "rg-medisure-dev"
$rgPrd = "rg-medisure-prod"

$wsDev = "dbw-medisure-dev"
$wsPrd = "dbw-medisure-prod"

# These can be new or empty existing RGs; CLI will create them if missing
$mrgDev = "rg-medisure-dev-dbw-managed"
$mrgPrd = "rg-medisure-prod-dbw-managed"

# --- 3) Create workspaces (Trial SKU, public network access enabled) ---
az databricks workspace create `
    --name $wsDev `
    --resource-group $rgDev `
    --location $loc `
    --sku trial `
    --managed-resource-group $mrgDev `
    --public-network-access Enabled `
    --tags env=dev project=medisure

<# az databricks workspace create `
    --name $wsPrd `
    --resource-group $rgPrd `
    --location $loc `
    --sku trial `
    --managed-resource-group $mrgPrd `
    --public-network-access Enabled `
    --tags env=prod project=medisure #>

# --- 4) Verify + grab URLs ---
az databricks workspace show -g $rgDev -n $wsDev --query "{name:name,state:provisioningState,url:workspaceUrl}" -o table
#az databricks workspace show -g $rgPrd -n $wsPrd --query "{name:name,state:provisioningState,url:workspaceUrl}" -o table
