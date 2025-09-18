# --- 0) Login and pick subscription (interactive) ---
az login | Out-Null
$sub = Read-Host "Enter Subscription ID or Name"
az account set --subscription $sub
az account show --query "{user:user.name, sub:name, id:id}" -o table

# --- 1) Settings (edit to your naming/region) ---
$loc = "southeastasia"
$rgDev = "rg-medisure-dev"
$rgPrd = "rg-medisure-prod"

# Storage account names: 3-24 chars, lowercase, globally unique, letters+digits only
$saDev = "samedisuredev"
$saPrd = "samedisureprod"

# (Optional) Check name availability
az storage account check-name --name $saDev -o table
az storage account check-name --name $saPrd -o table

# --- 2) Create ADLS Gen2 accounts ---
# Dev: LRS (typical for non-prod)
az storage account create `
    --name $saDev `
    --resource-group $rgDev `
    --location $loc `
    --sku Standard_LRS `
    --kind StorageV2 `
    --hierarchical-namespace true `
    --allow-blob-public-access false `
    --min-tls-version TLS1_2 `
    --tags env=dev project=medisure

# Prod: GRS (example; adjust to your policy)
<# az storage account create `
    --name $saPrd `
    --resource-group $rgPrd `
    --location $loc `
    --sku Standard_GRS `
    --kind StorageV2 `
    --hierarchical-namespace true `
    --allow-blob-public-access false `
    --min-tls-version TLS1_2 `
    --tags env=prod project=medisure #>

# --- 3) Verify ---
az storage account show --name $saDev --resource-group $rgDev -o table
#az storage account show --name $saPrd --resource-group $rgPrd -o table



