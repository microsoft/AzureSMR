#' -----------------------------------------------------------------------------
#' DESCRIPTION:
#' This is a manual test for testing device code flow of authentication, since 
#' this waits till the user can manually authenticate the provided DeviceCode. 
#' 
#' TO RUN:
#' ** Set correct path of "config_devicecode.json" in the below "settingsfile".
#' ** setwd("<root>/AzureSMR/tests/")
#' ** test_check("AzureSMR", filter = "1-authentication-devicecode")
#' 
#' SAMPLE config_devicecode.json:
#' 
#' {
#' "authType": "DeviceCode",
#' "resource": "https://datalake.azure.net/",
#' "tenantID": "72fblahf-blah-41af-blah-2d7cBLAHdb47",
#' "clientID": "2dblah05-blah-4e0a-blah-ae4d2BLAH5df",
#' }
#' -----------------------------------------------------------------------------


if(interactive()) library("testthat")

settingsfile <- find_config_json()

#  ------------------------------------------------------------------------

context("Authenticate")

test_that("Can connect to workspace with config file using device code", {
  skip_if_missing_config(settingsfile)
  
  config <- read.AzureSMR.config(settingsfile)
  asc <- createAzureContext()
  with(config,
       setAzureContext(asc, tenantID = tenantID, clientID = clientID, authType = authType, resource = resource)
  )
  expect_is(asc, "azureActiveContext")
  
  expect_is(asc$authType, "DeviceCode")
  
  res <- azureAuthenticateOnAuthType(asc)
  expect_true(res)
  
  expect_gt(as.numeric(asc$EXPIRY), as.numeric(Sys.time() + 3590))
})

