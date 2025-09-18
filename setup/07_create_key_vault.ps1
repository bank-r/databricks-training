$loc   = "southeastasia"
$rgDev = "rg-medisure-dev"
$rgPrd = "rg-medisure-prod"
# Key Vault names: globally unique, letters/digits/hyphen, start with letter
$kvDev = "kv-medisure-dev"
$kvPrd = "kv-medisure-prod"

# --- 2) Create Key Vaults
az keyvault create `
    --name $kvDev --resource-group $rgDev --location $loc --sku standard `
    --public-network-access Enabled `

# az keyvault create `
#   --name $kvPrd --resource-group $rgPrd --location $loc --sku standard `
#   --public-network-access Enabled `
#   --enable-purge-protection true   # recommended for prod

# --- 3) Grant access (current user + AzureDatabricks Enterprise App) ---
$meObjId   = az ad signed-in-user show --query id -o tsv
$adbSpObj  = az ad sp show --id 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d --query id -o tsv   # "AzureDatabricks" app

# Prompt for SQL password securely
$sec  = Read-Host "Enter SQL admin password for dev secret" -AsSecureString
$ptr  = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($sec)
$sqlPwd = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($ptr)
[Runtime.InteropServices.Marshal]::ZeroFreeBSTR($ptr)

az keyvault secret set --vault-name $kvDev --name "sql-user" --value "sqladmin" | Out-Null
az keyvault secret set --vault-name $kvDev --name "sql-pass" --value "$sqlPwd"       | Out-Null

# --- 5) Output values needed for Databricks secret scope creation ---
$kvDevUri = az keyvault show -n $kvDev -g $rgDev --query properties.vaultUri -o tsv
$kvDevId  = az keyvault show -n $kvDev -g $rgDev --query id -o tsv
"$kvDevUri"
"$kvDevId"
