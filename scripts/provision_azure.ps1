<#
.SYNOPSIS
    One-time provisioning of all Azure resources for the stock ETL pipeline.

.DESCRIPTION
    Creates: resource group, storage account + bronze container, Azure SQL
    server + serverless free-tier database, Function App, and a service
    principal for GitHub Actions deployment.

    Prerequisites:
      - Azure CLI installed (winget install Microsoft.AzureCLI)
      - Logged in: az login
      - An active subscription: az account show

    Estimated cost: ~$0/month idle.
      - Azure SQL free tier: 100,000 vCore-seconds/month free, auto-pauses
      - Blob Storage: < $0.05/month at this data volume
      - Functions Consumption plan: 1M executions/month free

    Run each section manually the first time so you understand what it does.
#>

# ── Variables — change these to your own values ───────────────────────────────
$LOCATION = "eastus2"                        # pick a region near you
$RG = "rg-stock-etl"                         # resource group name
$STORAGE = "ststocketl$(Get-Random -Maximum 9999)"  # must be globally unique, lowercase
$SQL_SERVER = "sql-stock-etl-$(Get-Random -Maximum 9999)"  # globally unique
$SQL_DB = "stockmarket"
$SQL_ADMIN = "etladmin"
$SQL_PASSWORD = Read-Host "Enter a strong SQL admin password" -AsSecureString
$SQL_PASSWORD_PLAIN = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
    [Runtime.InteropServices.Marshal]::SecureStringToBSTR($SQL_PASSWORD))
$FUNC_APP = "func-stock-etl-$(Get-Random -Maximum 9999)"   # globally unique

# ── 1. Resource group (free — just a logical container) ──────────────────────
az group create --name $RG --location $LOCATION

# ── 2. Storage account + bronze container (~$0.02/GB/month) ──────────────────
az storage account create `
    --name $STORAGE `
    --resource-group $RG `
    --location $LOCATION `
    --sku Standard_LRS `
    --kind StorageV2

az storage container create `
    --name bronze `
    --account-name $STORAGE `
    --auth-mode login

# Print the connection string — put this in .env as AZURE_STORAGE_CONNECTION_STRING
az storage account show-connection-string --name $STORAGE --resource-group $RG --output tsv

# ── 3. Azure SQL server + serverless free-tier database ──────────────────────
# The free tier gives 100,000 vCore-seconds/month and auto-pauses when idle.
az sql server create `
    --name $SQL_SERVER `
    --resource-group $RG `
    --location $LOCATION `
    --admin-user $SQL_ADMIN `
    --admin-password $SQL_PASSWORD_PLAIN

az sql db create `
    --name $SQL_DB `
    --server $SQL_SERVER `
    --resource-group $RG `
    --edition GeneralPurpose `
    --compute-model Serverless `
    --family Gen5 `
    --capacity 1 `
    --use-free-limit `
    --free-limit-exhaustion-behavior AutoPause

# Allow Azure services (the Function App) to reach the server
az sql server firewall-rule create `
    --name AllowAzureServices `
    --server $SQL_SERVER `
    --resource-group $RG `
    --start-ip-address 0.0.0.0 `
    --end-ip-address 0.0.0.0

# Allow YOUR current IP for local development / Power BI Desktop
$MY_IP = (Invoke-RestMethod -Uri "https://api.ipify.org")
az sql server firewall-rule create `
    --name AllowMyIP `
    --server $SQL_SERVER `
    --resource-group $RG `
    --start-ip-address $MY_IP `
    --end-ip-address $MY_IP

# Your DATABASE_URL for .env (URL-encode the password if it has special chars):
Write-Host "`nDATABASE_URL=mssql+pyodbc://${SQL_ADMIN}:<password>@${SQL_SERVER}.database.windows.net:1433/${SQL_DB}?driver=ODBC+Driver+18+for+SQL+Server`n"

# ── 4. Function App (Consumption plan — 1M executions/month free) ─────────────
az functionapp create `
    --name $FUNC_APP `
    --resource-group $RG `
    --storage-account $STORAGE `
    --consumption-plan-location $LOCATION `
    --runtime python `
    --runtime-version 3.11 `
    --functions-version 4 `
    --os-type Linux

# ── 5. Service principal for GitHub Actions CD ────────────────────────────────
# Paste the JSON output into a GitHub repo secret named AZURE_CREDENTIALS:
#   Repo → Settings → Secrets and variables → Actions → New repository secret
$SUB_ID = az account show --query id --output tsv
az ad sp create-for-rbac `
    --name "sp-stock-etl-github" `
    --role Contributor `
    --scopes "/subscriptions/$SUB_ID/resourceGroups/$RG" `
    --json-auth

Write-Host "`nDone. Next steps:"
Write-Host "  1. Copy the storage connection string into .env"
Write-Host "  2. Copy DATABASE_URL into .env (and Function App settings)"
Write-Host "  3. Save the service principal JSON as the AZURE_CREDENTIALS GitHub secret"
Write-Host "  4. Set Function App settings: az functionapp config appsettings set"
