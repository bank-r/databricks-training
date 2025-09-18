$saDev = "samedisuredev"

# Create the ADLS Gen2 filesystem (container) for landing
az storage container create `
    --account-name $saDev `
    --name landing `
    --auth-mode login `
    --public-access off

# Create the ADLS Gen2 filesystem (container) for Unity Catalog managed files
az storage container create `
    --account-name $saDev `
    --name managed-delta-files `
    --auth-mode login `
    --public-access off

# --- Ensure the folders exist for a landing ---
az storage fs directory create --account-name $saDev --file-system landing --name "claims" --auth-mode login
az storage fs directory create --account-name $saDev --file-system landing --name "claims/batch" --auth-mode login
az storage fs directory create --account-name $saDev --file-system landing --name "claims/stream" --auth-mode login
az storage fs directory create --account-name $saDev --file-system landing --name "diagnosis" --auth-mode login
az storage fs directory create --account-name $saDev --file-system landing --name "providers" --auth-mode login

az storage fs file upload --account-name $saDev --file-system landing --path "claims/batch/claims_batch.csv" --source "data\claims_batch.csv" --auth-mode login --overwrite
az storage fs file upload --account-name $saDev --file-system landing --path "claims/stream/claims_stream.json" --source "data\claims_stream.json" --auth-mode login --overwrite
az storage fs file upload --account-name $saDev --file-system landing --path "diagnosis/diagnosis_ref.csv" --source "data\diagnosis_ref.csv" --auth-mode login --overwrite
az storage fs file upload --account-name $saDev --file-system landing --path "providers/providers.json" --source "data\providers.json" --auth-mode login --overwrite