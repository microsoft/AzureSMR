if (interactive()) library("testthat")


settingsfile <- system.file("tests/testthat/config.json", package = "AzureSMR")
config <- read.AzureSMR.config(settingsfile)

#  ------------------------------------------------------------------------

context("Scalesets")


asc <- createAzureContext()
with(config,
     setAzureContext(asc, tenantID = tenantID, clientID = clientID, authKey = authKey)
)


azureAuthenticate(asc, verbose = FALSE)

timestamp <- format(Sys.time(), format = "%y%m%d%H%M")
resourceGroup_name <- paste0("_AzureSMtest_", timestamp)

#test_that("Can create resource group", {
  #skip_if_missing_config(settingsfile)

  #res <- azureCreateResourceGroup(asc, location = "westeurope", resourceGroup = resourceGroup_name)
  #expect_true(res)

  #wait_for_azure(
    #resourceGroup_name %in% azureListRG(asc)$resourceGroup
  #)
  #expect_true(resourceGroup_name %in% azureListRG(asc)$resourceGroup)
#})

azureCreateHDI(asc)

azureListScaleSets(asc)
azureListScaleSetNetwork(asc)
azureListScaleSetVM(asc, scaleSet = "swarm-agent-D2D9AE69-vmss", resourceGroup = "THDELTEI-TEST-CONTAINER2")
azureListScaleSetVM(asc, scaleSet = "swarm-agent-D2D9AE69-vmss")

