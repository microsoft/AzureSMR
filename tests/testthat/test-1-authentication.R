if(interactive()) library("testthat")


settingsFile <- system.file("tests/testthat/config.json", package = "AzureSM")
config <- read.AzureSM.config(settingsFile)

#  ------------------------------------------------------------------------

context("Authenticate")



test_that("Can authenticate to Azure Service Manager API", {
  AzureSM:::skip_if_missing_config(settingsFile)

  asc <- CreateAzureContext()
  expect_is(asc, "environment")

  with(config,
       SetAzureContext(asc, TID = TID, CID = CID, KEY = KEY)
  )
  res <- AzureAuthenticate(asc)
  expect_equal(res, "Authentication Suceeded : Key Obtained")

})

asc <- CreateAzureContext()
with(config,
     SetAzureContext(asc, TID = TID, CID = CID, KEY = KEY)
)
AzureAuthenticate(asc)


test_that("Can connect to workspace with config file", {
  AzureSM:::skip_if_missing_config(settingsFile)

  res <- AzureListSubscriptions(asc)
  expect_is(res, "data.frame")
  expect_equal(ncol(res), 7)

  res <- AzureCheckToken(asc)
  expect_equal(res, "OK")

  res <- AzureListSA(asc, ResourceGroup = "advdsvmlinux1")
  expect_is(res, "data.frame")
  expect_equal(ncol(res), 8)

})


