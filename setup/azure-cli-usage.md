# Azure CLI Commands

## Login with browser

```powershell
az login
```

## pick subscription

```powershell
az account set --subscription "<SUBSCRIPTION_NAME_OR_ID>"
```

## verify account

```powershell
az account show --query "{user:user.name, subscription:name, id:id}" --output table
```

## Check if a name already exists (returns true/false)

```powershell
az group exists --name rg-medisure-dev
```

## List resource groups (table view)

```powershell
az group list --query "[].{Name:name, Location:location, Tags:tags}" --output table
```

## Update or add tags later

```powershell
az group update --name rg-medisure-dev --set tags.env=dev
```

## Delete (with no prompt)

az group delete --name rg-medisure-dev --yes --no-wait
