# ----------------------------------------------------------------- 
# Test for cost functions.
# ----------------------------------------------------------------- 

# preambles.

if (interactive()) library("testthat")

settingsfile <- getOption("AzureSMR.config")
config <- read.AzureSMR.config()

# setup.

context("Data consumption and cost")

asc <- createAzureContext()
with(config,
     setAzureContext(asc, tenantID=tenantID, clientID=clientID, authKey=authKey)
)
azureAuthenticate(asc)

timestamp <- format(Sys.time(), format="%y%m%d%H%M")
resourceGroup_name <- paste0("AzureSMtest_", timestamp)
sa_name <- paste0("azuresmr", timestamp)

time_end   <- paste(as.Date(Sys.Date()), "00:00:00", sep = " ")
time_start <- paste(as.Date(Sys.Date() - 365), "00:00:00", sep = " ")
  
# run test.

# get data consumption by day.

test_that("Get data consumption by day", {
  skip_if_missing_config(settingsfile)
  
  res <- azureDataConsumption(azureActiveContext=asc,
                              timeStart=time_start,
                              timeEnd=time_end,
                              granularity="Daily")
  
  expect_is(res, class="data.frame")
  expect_identical(object=names(res), expected=c("usageStartTime", 
                                                 "usageEndTime",
                                                 "meterName", 
                                                 "meterCategory",
                                                 "meterSubCategory",
                                                 "unit",
                                                 "meterId",
                                                 "quantity",
                                                 "meterRegion"))
})

# get pricing rates for meters under subscription.

test_that("Get pricing rates", {
  skip_if_missing_config(settingsfile)
  
  res <- azurePricingRates(azureActiveContext=asc,
                           currency=config$CURRENCY,
                           locale=config$LOCALE,
                           offerId=config$OFFER,
                           region=config$REGION)
  
  expect_is(res, class="data.frame")
  expect_identical(object=names(res), expected=c("effectiveDate", 
                                                 "includedQuantity",
                                                 "meterCategory",
                                                 "meterId",
                                                 "meterName",
                                                 "meterRegion",
                                                 "meterStatus",
                                                 "meterSubCategory",
                                                 "unit",
                                                 "meterRate"))
})


# total expense by day.

test_that("Get cost by day", {
  skip_if_missing_config(settingsfile)
  
  res <- azureExpenseCalculator(azureActiveContext=asc,
                                timeStart=time_start,
                                timeEnd=time_end,
                                granularity="Daily",
                                currency=config$CURRENCY,
                                locale=config$LOCALE,
                                offerId=config$OFFER,
                                region=config$REGION)
  
  expect_is(res, class="data.frame")
  expect_identical(object=names(res), expected=c("meterName",
                                                 "meterCategory",
                                                 "meterSubCategory",
                                                 "quantity",
                                                 "unit",
                                                 "meterRate",
                                                 "cost"))
})