if (interactive()) library("testthat")


settingsfile <- system.file("tests/testthat/config.json", package = "AzureSMR")
config <- read.AzureSMR.config(settingsfile)

#  ------------------------------------------------------------------------

context("Virtual machine")

asc <- createAzureContext()
with(config,
     setAzureContext(asc, tenantID = tenantID, clientID = clientID, authKey = authKey)
)
azureAuthenticate(asc)


timestamp <- format(Sys.time(), format = "%y%m%d%H%M")
resourceGroup_name <- paste0("AzureSMtest_", timestamp)
sa_name <- paste0("azuresmr", timestamp)

#  ------------------------------------------------------------------------

test_that("Can create resource group", {
  skip_if_missing_config(settingsfile)

  res <- azureCreateResourceGroup(asc, location = "westeurope", resourceGroup = resourceGroup_name)
  expect_equal(res, TRUE)

  wait_for_azure(
    resourceGroup_name %in% azureListRG(asc)$resourceGroup
  )
  expect_true(resourceGroup_name %in% azureListRG(asc)$resourceGroup)
})


context(" - create VM")
test_that("Can create virtual machine from template", {
  skip_if_missing_config(settingsfile)

  azureTemplatesUrl <- "https://raw.githubusercontent.com/Azure/azure-quickstart-templates/master/"
  templateURL = paste0(azureTemplatesUrl, "101-vm-simple-linux/azuredeploy.json")

  paramJSON <- '"parameters": {
      "adminUsername":  {"value": "azuresmr"},
      "adminPassword":  {"value": "Azuresmrtest123!"},
      "dnsLabelPrefix": {"value": "azuresmr"}}'

  res <- azureDeployTemplate(asc, deplname = "Deploy2", templateURL = templateURL, paramJSON = paramJSON)
  expect_true(res)
})


context(" - stop VM")
test_that("Can stop a virtual machine", {
  res <- azureListVM(asc)
  expect_is(res, "data.frame")
  expect_equal(ncol(res), 7)

  res <- azureVMStatus(asc, vmName = "MyUbuntuVM")
  expect_equal(res, "Provisioning succeeded, VM running")

  res <- azureStopVM(asc, vmName = "MyUbuntuVM")
  expect_true(res)
})


context(" - delete VM")
test_that("Can delete virtual machine", {
  res <- azureVMStatus(asc, vmName = "MyUbuntuVM", ignore = "Y")
  expect_equal(res, "Provisioning succeeded, VM deallocated")


  res <- azureDeleteVM(asc, vmName = "MyUbuntuVM")
  expect_true(res)


})


#  ------------------------------------------------------------------------

context(" - delete resource group")
test_that("Can delete resource group", {
  skip_if_missing_config(settingsfile)

  expect_message({
      res <- azureDeleteResourceGroup(asc, resourceGroup = resourceGroup_name)
    }, "Delete Request Submitted"
  )
  wait_for_azure(
    !(resourceGroup_name %in% azureListRG(asc)$resourceGroup)
  )
  expect_false(resourceGroup_name %in% sort(azureListRG(asc)$resourceGroup))

})
