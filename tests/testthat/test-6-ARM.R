if (interactive()) library("testthat")

settingsfile <- system.file("tests/testthat/config.json", package = "AzureSMR")
config <- read.AzureSMR.config(settingsfile)

#  ------------------------------------------------------------------------

context("ARM")

asc <- createAzureContext()
with(config,
     setAzureContext(asc, tenantID = tenantID, clientID = clientID, authKey = authKey)
)

azureAuthenticate(asc, verbose = FALSE)

timestamp <- format(Sys.time(), format = "%y%m%d%H%M")
resourceGroup_name <- paste0("AzureSMtest_", timestamp)

test_that("Can create resource group", {
  skip_if_missing_config(settingsfile)

  res <- azureCreateResourceGroup(asc, location = "centralus", resourceGroup = resourceGroup_name)
  expect_true(res)

  wait_for_azure(
    resourceGroup_name %in% azureListRG(asc)$resourceGroup
  )
  expect_true(resourceGroup_name %in% azureListRG(asc)$resourceGroup)
})


paramJSON1 <- '"parameters": {"storageAccountType": {"value": "Standard_GRS"}}'
  
templateJSON1 <- '
{
  "$schema": "https://schema.management.azure.com/schemas/2015-01-01/deploymentTemplate.json#",
  "contentVersion": "1.0.0.0",
  "parameters": {
  "storageAccountType": {
  "type": "string",
  "defaultValue": "Standard_LRS",
  "allowedValues": [
    "Standard_LRS",
    "Standard_GRS",
    "Standard_ZRS",
    "Premium_LRS"
  ],
  "metadata": {"description": "Storage Account type"}
  }
  },
  "variables": {"storageAccountName": "[uniquestring(resourceGroup().id)]"},
  "resources": [
    {
      "type": "Microsoft.Storage/storageAccounts",
      "name": "[uniquestring(resourceGroup().id)]",
      "apiVersion": "2016-01-01",
      "location": "[resourceGroup().location]",
      "sku": {"name": "Standard_GRS"},
      "kind": "Storage", 
      "properties": {}
    }
  ],
  "outputs": {"storageAccountName": {"type": "string","value": "[uniquestring(resourceGroup().id)]"}}
  }
'

paramJSON2 <- '
"parameters" : {
  "vmssName": {"value": "azuresmrvmss"},
  "instanceCount": {"value": 2},
  "adminUsername": {"value": "ubuntu"},
  "adminPassword": {"value": "Password123"}
}'

context(" - deploy 1")
test_that("Can create resource from json", {
  res <- azureDeployTemplate(asc, deplname = "Deploy1",
    templateJSON = templateJSON1,
    paramJSON = paramJSON1,
    resourceGroup = resourceGroup_name,
    verbose = FALSE)
  expect_true(res)
})


context(" - deploy 2")
test_that("Can create resource from URL", {
  tempURL = "https://raw.githubusercontent.com/Azure/azure-quickstart-templates/master/201-vmss-linux-jumpbox/azuredeploy.json"
  res <- azureDeployTemplate(asc, deplname = "Deploy2",
     templateURL = tempURL, 
     paramJSON = paramJSON2, 
     resourceGroup = resourceGroup_name, 
     verbose = FALSE)
  expect_true(res)
})

test_that("Can delete resource group", {
  skip_if_missing_config(settingsfile)
  
  azureDeleteResourceGroup(asc, resourceGroup = resourceGroup_name)
  
})