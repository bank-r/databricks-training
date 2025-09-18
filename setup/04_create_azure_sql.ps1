# --- 0) Login (ARM scope) & pick subscription ---
# If you already logged in with scope, skip this.
#az login --use-device-code --tenant <YOUR_TENANT> --scope https://management.core.windows.net//.default
#$sub = Read-Host "Enter Subscription ID or Name"
#az account set --subscription $sub

# --- 1) Variables ---
$loc    = "southeastasia"
$rgDev  = "rg-medisure-dev"
$rgPrd  = "rg-medisure-prod"
$sqlDev = "sql-medisure-dev"    # must be globally unique per region+name
$sqlPrd = "sql-medisure-prod"
$dbDev  = "db_medisure_dev"
$dbPrd  = "db_medisure_prod"
$sqlAdmin = "sqladmin"      # change

# Prompt for a strong SQL admin password (pass it to CLI safely)
$sec  = Read-Host "Enter SQL admin password" -AsSecureString
$ptr  = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($sec)
$sqlPwd = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($ptr)
[Runtime.InteropServices.Marshal]::ZeroFreeBSTR($ptr)

# --- 2) Create SQL servers ---
az sql server create -g $rgDev -n $sqlDev -l $loc -u $sqlAdmin -p $sqlPwd
#az sql server create -g $rgPrd -n $sqlPrd -l $loc -u $sqlAdmin -p $sqlPwd

# --- 3) Create databases ---
az sql db create -g $rgDev -s $sqlDev -n $dbDev `
    --compute-model Serverless --edition GeneralPurpose --family Gen5 --capacity 1 `
    --auto-pause-delay 15 `
    --backup-storage-redundancy Local `
    --capacity 8

# Prod: empty DB, same SKU
# az sql db create -g $rgPrd -s $sqlPrd -n $dbPrd `
#     --compute-model Serverless --edition GeneralPurpose --family Gen5 --capacity 1 `
#     --auto-pause-delay 60

# --- 4) Firewall (quick & simple for Databricks) ---
# Allow Azure services (needed because workspace egress IPs are dynamic with public networking)
az sql server firewall-rule create -g $rgDev -s $sqlDev -n AllowAzure --start-ip-address 0.0.0.0 --end-ip-address 0.0.0.0
#az sql server firewall-rule create -g $rgPrd -s $sqlPrd -n AllowAzure -start-ip-address 0.0.0.0 -end-ip-address 0.0.0.0

# Also allow your current public IP for admin tools
$myIp = (Invoke-RestMethod "https://ifconfig.me/ip")
az sql server firewall-rule create -g $rgDev -s $sqlDev -n AllowMyIP --start-ip-address $myIp --end-ip-address $myIp

# --- 5) Verify ---
az sql db show -g $rgDev -s $sqlDev -n $dbDev --query "{name:name,status:status,sku:sku.name}" -o table
#az sql db show -g $rgPrd -s $sqlPrd -n $dbPrd --query "{name:name,status:status,sku:sku.name}" -o table
