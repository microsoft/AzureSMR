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


# --------

context(" - HDI validation")

test_that("Can create HDI cluster", {
  skip_if_missing_config(settingsfile)
  expect_error(
    azureCreateHDI(asc, resourceGroup = resourceGroup_name),
    "clustername"
  )
  expect_error(
    azureCreateHDI(asc, resourceGroup = resourceGroup_name, clustername = "azuresmr_hdi_test"),
    "storageAccount"
  )
  expect_message(
    azureCreateHDI(asc, resourceGroup = resourceGroup_name, clustername = "azuresmr_hdi_test",
    storageAccount = "azuresmrhditest",
    adminUser = "Azuresmr_test1", adminPassword = "Password_1",
    sshUser = "sssUser_test1", sshPassword = "Password_1",
    debug = TRUE
    ),
    "creating storage account"
  )

  expect_error(
    azureCreateHDI(asc, resourceGroup = resourceGroup_name, clustername = "azuresmr_hdi_test",
    storageAccount = "azuresmrhditest",
    adminUser = "Azuresmr_test1", adminPassword = "Azuresmr_test1",
    sshUser = "sssUser_test1", sshPassword = "sssUser_test1",
    debug = FALSE
    ),
    "should not contain 3 consecutive letters from the username"
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
})

# --------

context(" - create cluster with rserver")

test_that("can create HDI cluster", {

  # create the actual instance - rserver
  expect_true(
    azureCreateHDI(asc, resourceGroup = resourceGroup_name, clustername = "azuresmrhditest",
        storageAccount = paste0("azuresmrhdi", timestamp),
        adminUser = "x", adminPassword = "Azuresmr_test1",
        sshUser = "sssUser_test1", sshPassword = "sshUser_test1",
        kind = "rserver",
        debug = FALSE
        )
    )
})


# --------

context(" - run action script")

test_that("can run action scripts", {
  # run an action script
  expect_true(
    azureRunScriptAction(asc,
                       scriptname = "installPackages",
                       scriptURL = "http://mrsactionscripts.blob.core.windows.net/rpackages-v01/InstallRPackages.sh",
                       workerNode = TRUE, edgeNode = TRUE,
                       parameters = "useCRAN stringr")
  )

  # retrieve action script history
  z <- azureScriptActionHistory(asc)
  expect_is(z, "data.frame")
})

# --------

context(" - delete cluster")

test_that("can delete HDI cluster", {
  # list clusters
  z <- azureListHDI(asc)
  expect_is(z, "data.frame")

  z <- azureListHDI(asc, clustername = "azuresmrhditest", resourceGroup = resourceGroup_name)
  expect_is(z, "data.frame")

  # delete cluster
  expect_true(
    azureDeleteHDI(asc, clustername = "azuresmrhditest")
  )

  azureDeleteResourceGroup(asc, resourceGroup = resourceGroup_name)
})


