if (interactive()) library("testthat")


settingsfile <- system.file("tests/testthat/config.json", package = "AzureSMR")
config <- read.AzureSMR.config(settingsfile)

#  ------------------------------------------------------------------------

context("HDI")


asc <- createAzureContext()
with(config,
     setAzureContext(asc, tenantID = tenantID, clientID = clientID, authKey = authKey)
)


azureAuthenticate(asc, verbose = FALSE)

timestamp <- format(Sys.time(), format = "%y%m%d%H%M")
resourceGroup_name <- paste0("_AzureSMtest_", timestamp)

test_that("Can create resource group", {
  skip_if_missing_config(settingsfile)

  res <- azureCreateResourceGroup(asc, location = "centralus", resourceGroup = resourceGroup_name)
  expect_true(res)

  wait_for_azure(
    resourceGroup_name %in% azureListRG(asc)$resourceGroup
  )
  expect_true(resourceGroup_name %in% azureListRG(asc)$resourceGroup)
})

azureListHDI(asc)

context(" - create HDI cluster")

test_that("Can create HDI cluster", {
  skip_if_missing_config(settingsfile)
  expect_error(
    azureCreateHDI(asc),
    "resourceGroup"
  )
  expect_error(
    azureCreateHDI(asc, resourceGroup = resourceGroup_name),
    "clustername"
  )
  expect_error(
    azureCreateHDI(asc, resourceGroup = resourceGroup_name, clustername = "azuresmr_hdi_test"),
    "storageAccount"
  )
  expect_error(
    azureCreateHDI(asc, resourceGroup = resourceGroup_name, clustername = "azuresmr_hdi_test",
    storageAccount = "azuresmrhditest",
    adminUser = "Azuresmr_test1", adminPassword = "Azuresmr_test1",
    sshUser = "sssUser_test1", sshPassword = "sshUser_test1"
    ),
    "storageAccount"
  )

  # debug - default
  expect_is(
    azureCreateHDI(asc, resourceGroup = resourceGroup_name, clustername = "azuresmr_hdi_test",
      storageAccount = "azuresmrhditest",
      adminUser = "x", adminPassword = "Azuresmr_test1",
      sshUser = "sssUser_test1", sshPassword = "sshUser_test1",
      debug = TRUE
      ),
      "list"
  )

  # debug - rserver
  expect_is(
    azureCreateHDI(asc, resourceGroup = resourceGroup_name, clustername = "azuresmr_hdi_test",
      storageAccount = "azuresmrhditest",
      adminUser = "x", adminPassword = "Azuresmr_test1",
      sshUser = "sssUser_test1", sshPassword = "sshUser_test1",
      kind = "rserver",
      debug = TRUE
      ),
      "list"
  )

  # debug - hadoop
  expect_is(
    azureCreateHDI(asc, resourceGroup = resourceGroup_name, clustername = "azuresmr_hdi_test",
      storageAccount = "azuresmrhditest",
      adminUser = "x", adminPassword = "Azuresmr_test1",
      sshUser = "sssUser_test1", sshPassword = "sshUser_test1",
      kind = "hadoop",
      debug = TRUE
      ),
      "list"
  )

  # create the actual instance - rserver
  expect_true(
  azureCreateHDI(asc, resourceGroup = resourceGroup_name, clustername = "azuresmrhditest",
      storageAccount = "azuresmrhditest",
      adminUser = "x", adminPassword = "Azuresmr_test1",
      sshUser = "sssUser_test1", sshPassword = "sshUser_test1",
      kind = "rserver",
      debug = FALSE
      )
    )

  azureListHDI(asc)
  azureDeleteHDI(asc, clustername = "azuresmrhditest")
  azureListHDI(asc)
  pollStatusHDI(asc, clustername = "azuresmrhditest")
  azureDeleteResourceGroup(asc, resourceGroup = resourceGroup_name)
})


