if(interactive()) library("testthat")


settingsfile <- system.file("tests/testthat/config.json", package = "AzureSMR")
config <- read.AzureSMR.config(settingsfile)

#  ------------------------------------------------------------------------

context("Authenticate")



test_that("Can authenticate to Azure Service Manager API", {
  skip_if_missing_config(settingsfile)

  asc <- createAzureContext()
  expect_is(asc, "azureActiveContext")

  with(config,
       setAzureContext(asc, tenantID = tenantID, clientID = clientID, authKey = authKey)
  )
  res <- azureAuthenticate(asc)
  expect_equal(res, "Authentication Suceeded : Key Obtained")

})

asc <- createAzureContext()
with(config,
     setAzureContext(asc, tenantID = tenantID, clientID = clientID, authKey = authKey)
)
azureAuthenticate(asc)


test_that("Can connect to workspace with config file", {
  skip_if_missing_config(settingsfile)

  res <- azureListSubscriptions(asc)
  expect_is(res, "data.frame")
  expect_equal(ncol(res), 7)

  res <- azureCheckToken(asc)
  expect_equal(res, "OK")

  res <- azureListSA(asc, resourceGroup = "advdsvmlinux1")
  expect_is(res, "data.frame")
  expect_equal(ncol(res), 8)

})


