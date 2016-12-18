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


test_that("Can connect to azure resources", {
  AzureSM:::skip_if_missing_config(settingsFile)

  res <- AzureListAllResources(asc)
  expect_is(res, "data.frame")
  expect_equal(ncol(res), 5)

  expect_error(AzureListAllRecources(asc)) # Deprecated function

  res <- AzureListAllResources(asc)
  expect_is(res, "data.frame")
  expect_equal(ncol(res), 5)

  res <- AzureListRG(asc)
  expect_is(res, "data.frame")
  expect_equal(ncol(res), 3)

  # wait until request is completed
  wait_for_azure <- function(expr, test, pause = 5, times = 10){
    terminate <- FALSE
    .counter <- 0
    while(!terminate && .counter <= times){
      .counter <- .counter + 1
      Sys.sleep(pause)
      res <- eval(expr)
      terminate <- !isTRUE(eval(test))
    }
    res
  }

  rg_name <- "_AzureSM-package-test"
  res <- AzureCreateResourceGroup(asc, Location = "westeurope", ResourceGroup = rg_name)
  expect_equal(res, "Create Request Submitted")

  res <- wait_for_azure(
    asc %>% AzureListRG() %>% .$ID %>% basename(),
    rg_name %in% res
  )
  expect_true(rg_name %in% res)

  res <- AzureDeleteResourceGroup(asc, ResourceGroup = rg_name)
  expect_equal(res, "Delete Request Submitted")
  res <- wait_for_azure(
    asc %>% AzureListRG() %>% .$ID %>% basename(),
    !(rg_name %in% res)
  )
  expect_false(rg_name %in% res)

})


