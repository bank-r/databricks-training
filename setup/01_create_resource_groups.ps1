# 1) Login and pick a subscription (if needed)
az login

# Prompt for subscription (ID or Name both work)
$sub = Read-Host "Enter Subscription ID or Name"
az account set --subscription $sub
az account show --query "{user:user.name, sub:name, id:id}" -o table

# 2) Create a resource group (idempotent)
az group create `
    --name rg-medisure-dev `
    --location southeastasia `
    --tags env=dev project=medisure

<# az group create `
    --name rg-medisure-prod `
    --location southeastasia `
    --tags env=prod project=medisure #>

# 3) Verify
az group show --name rg-medisure-dev --output table
#az group show --name rg-medisure-prod --output table


