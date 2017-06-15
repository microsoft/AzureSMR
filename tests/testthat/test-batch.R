if (interactive()) library("testthat")

settingsfile <- system.file("tests/testthat/config.json", package = "AzureSMR")
config <- read.AzureSMR.config(settingsfile)

#  ------------------------------------------------------------------------

context("Batch")

asc <- createAzureContext()
with(config,
    setAzureContext(asc, tenantID = tenantID, clientID = clientID, authKey = authKey)
)

azureAuthenticate(asc)

timestamp <- format(Sys.time(), format = "%y%m%d%H%M")
resourceGroup_name <- paste0("AzureSMtest_", timestamp)
batch_account <- paste0("azuresmr", timestamp)
batch_location = "westeurope"

test_that("Can create resource group", {
  skip_if_missing_config(settingsfile)

  res <- azureCreateResourceGroup(asc, location = "westeurope", resourceGroup = resourceGroup_name)
  expect_equal(res, TRUE)

  wait_for_azure(
    resourceGroup_name %in% azureListRG(asc)$resourceGroup
  )
  expect_true(resourceGroup_name %in% azureListRG(asc)$resourceGroup)
})


context(" - batch account")
test_that("create batch account", {
  skip_if_missing_config(settingsfile)
  
  res <- azureCreateBatchAccount(asc,
                                 batchAccount = batch_account,
                                 resourceGroup = resourceGroup_name,
                                 location = batch_location)
  
  if(res == "Account already exists with the same name") skip("Account already exists with the same name")
  expect_equal(res, TRUE)
  
  wait_for_azure(
    batch_account %in% azureListBatchAccounts(asc)$name
  )
  
  expect_true(batch_account %in% azureListBatchAccounts(asc)$name)
})

context(" - batch account list keys")
test_that("list keys", {
  skip_if_missing_config(settingsfile)
  
  wait_for_azure(
    batch_account %in% azureListBatchAccounts(asc)$name
  )
  
  res <- azureBatchGetKey(asc,
                          batchAccount = batch_account,
                          resourceGroup = resourceGroup_name)
  
  expect_true(is_storage_key(res))
})

context(" - delete batch account")
test_that("can delete batch account", {
  skip_if_missing_config(settingsfile)
  
  # delete the actual batch account
  expect_true(
    azureDeleteBatchAccount(asc,
                            batchAccount = batch_account,
                            resourceGroup = resourceGroup_name, 
                            subscriptionID = asc$subscriptionID)
  )
  
  azureDeleteResourceGroup(asc, resourceGroup = resourceGroup_name)
})