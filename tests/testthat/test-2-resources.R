if(interactive()) library("testthat")


settingsFile <- system.file("tests/testthat/config.json", package = "AzureSM")
config <- read.AzureSM.config(settingsFile)

#  ------------------------------------------------------------------------

context("Azure resources")

asc <- CreateAzureContext()
with(config,
     SetAzureContext(asc, TID = TID, CID = CID, KEY = KEY)
)
AzureAuthenticate(asc)


timestamp          <- format(Sys.time(), format = "%y%m%d%H%M")
resourceGroup_name <- paste0("_AzureSMtest_", timestamp)
sa_name            <- paste0("azuresmr", timestamp)


test_that("Can connect to azure resources", {
  AzureSM:::skip_if_missing_config(settingsFile)

  res <- AzureListAllResources(asc)
  expect_is(res, "data.frame")
  expect_equal(ncol(res), 7)

  expect_error(AzureListAllRecources(asc)) # Deprecated function

  res <- AzureListAllResources(asc)
  expect_is(res, "data.frame")
  expect_equal(ncol(res), 7)

  res <- AzureListRG(asc)
  expect_is(res, "data.frame")
  expect_equal(ncol(res), 4)
})


test_that("Can create resource group", {
  AzureSM:::skip_if_missing_config(settingsFile)

  res <- AzureCreateResourceGroup(asc, Location = "westeurope", ResourceGroup = resourceGroup_name)
  expect_equal(res, "Create Request Submitted")

  wait_for_azure(
    resourceGroup_name %in% AzureListRG(asc)$ResourceGroup
  )
  expect_true(resourceGroup_name %in% AzureListRG(asc)$ResourceGroup)
})


test_that("Can connect to storage account", {
  AzureSM:::skip_if_missing_config(settingsFile)

  res <- AzureListSA(asc)
  expect_is(res, "data.frame")
  expect_equal(ncol(res), 8)

  sub_id  <<- res$StorageAccount[1]
  rg_temp <<- res$ResourceGroup[1]
  res <- AzureSAGetKey(asc, StorageAccount = sub_id, ResourceGroup = rg_temp)
  expect_is(res, "character")
})

test_that("Can create storage account", {
  AzureSM:::skip_if_missing_config(settingsFile)

  res <- AzureCreateStorageAccount(asc, StorageAccount = sa_name, ResourceGroup = resourceGroup_name)
  if(res == "Account already exists with the same name") skip("Account already exists with the same name")
  expect_equal(res, "Create request Accepted. It can take a few moments to provision the storage account")

  wait_for_azure(
    sa_name %in% sort(AzureListSA(asc)$StorageAccount)
  )
  expect_true(sa_name %in% AzureListSA(asc)$StorageAccount)
})

test_that("Can connect to container", {
  AzureSM:::skip_if_missing_config(settingsFile)
  sa <- AzureListSA(asc)[1, ]
  res <- AzureListSAContainers(asc, StorageAccount = sa$StorageAccount[1], ResourceGroup = sa$ResourceGroup[1])
  expect_is(res, "data.frame")
  expect_equal(ncol(res), 5)
})



test_that("Can delete storage account", {
  AzureSM:::skip_if_missing_config(settingsFile)

  res <- AzureDeleteStorageAccount(asc, StorageAccount = sa_name, ResourceGroup = resourceGroup_name)
  expect_equal(res, "Done")
  wait_for_azure(
    !(sa_name %in% AzureListSA(asc)$StorageAccount)
  )
  expect_false(sa_name %in% sort(AzureListSA(asc)$StorageAccount))
})

test_that("Can delete resource group", {
  AzureSM:::skip_if_missing_config(settingsFile)

  res <- AzureDeleteResourceGroup(asc, ResourceGroup = resourceGroup_name)
  expect_equal(res, "Delete Request Submitted")
  wait_for_azure(
    !(resourceGroup_name %in% AzureListRG(asc)$ResourceGroup)
  )
  expect_false(resourceGroup_name %in% sort(AzureListRG(asc)$ResourceGroup))

})



