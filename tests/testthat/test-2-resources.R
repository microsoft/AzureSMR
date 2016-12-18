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

rg_name <- "_AzureSM-package-test"

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
  expect_equal(ncol(res), 3)
})


test_that("Can create resource group", {
  AzureSM:::skip_if_missing_config(settingsFile)

  res <- AzureCreateResourceGroup(asc, Location = "westeurope", ResourceGroup = rg_name)
  expect_equal(res, "Create Request Submitted")

  res <- AzureSM:::wait_for_azure(
    asc %>% AzureListRG() %>% .$ID %>% basename(),
    rg_name %in% res
  )
  expect_true(rg_name %in% res)
})


test_that("Can connect to storage account", {
  AzureSM:::skip_if_missing_config(settingsFile)

  res <- AzureListSA(asc)
  expect_is(res, "data.frame")
  expect_equal(ncol(res), 8)

  sub_id  <<- res$StorageAccount[1]
  rg_name <<- res$ResourceGroup[1]
  res <- AzureSAGetKey(asc, StorageAccount = sub_id, ResourceGroup = rg_name)
  expect_is(res, "character")
})

test_that("Can create storage account", {
  AzureSM:::skip_if_missing_config(settingsFile)

  sa_name <- "storageazursmrtest"
  res <- AzureCreateStorageAccount(asc, StorageAccount = sa_name, ResourceGroup = rg_name)
  expect_equal(res, "Create request Accepted. It can take a few moments to provision the storage account")

  res <- AzureSM:::wait_for_azure(
    AzureListSA(asc)$StorageAccount,
    sa_name %in% res
  )
  expect_true(sa_name %in% res)
})

test_that("Can delete storage account", {
  AzureSM:::skip_if_missing_config(settingsFile)

  res <- AzureDeleteStorageAccount(asc, StorageAccount = sa_name, ResourceGroup = rg_name)
  expect_equal(res, "Done")
  expect_false(sa_name %in% AzureListSA(asc)$StorageAccount)

})

test_that("Can delete resource group", {
  AzureSM:::skip_if_missing_config(settingsFile)

  res <- AzureDeleteResourceGroup(asc, ResourceGroup = rg_name)
  expect_equal(res, "Delete Request Submitted")
  res <- AzureSM:::wait_for_azure(
    asc %>% AzureListRG() %>% .$ID %>% basename(),
    !(rg_name %in% res)
  )
  expect_false(rg_name %in% res)

})



