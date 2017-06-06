if(interactive()) library("testthat")

settingsfile <- system.file("tests/testthat/config.json", package = "AzureSMR")

#  ------------------------------------------------------------------------

context("Authenticate")



test_that("Can authenticate to Azure Service Manager API", {
  skip_if_missing_config(settingsfile)
  config <- read.AzureSMR.config(settingsfile)

  asc <- createAzureContext()
  expect_is(asc, "azureActiveContext")

  with(config,
       setAzureContext(asc, 
         tenantID = tenantID, 
         clientID = clientID, 
         authKey = authKey)
  )
  expect_true(azureAuthenticate(asc))

  asc <- createAzureContext(configFile = settingsfile)
  expect_is(asc, "azureActiveContext")



})

asc <- createAzureContext(configFile = settingsfile)

test_that("Can connect to workspace with config file", {
  skip_if_missing_config(settingsfile)

  res <- azureListSubscriptions(asc)
  expect_is(res, "data.frame")
  expect_equal(ncol(res), 7)

  res <- azureCheckToken(asc)
  expect_true(res)

  res <- azureListSA(asc, resourceGroup = "advdsvmlinux1")
  expect_is(res, "data.frame")
  expect_equal(ncol(res), 8)

})


