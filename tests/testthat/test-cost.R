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

# run test.

# get cost by day.

test_that("Get cost by day", {
  skip_if_missing_config(settingsfile)
  
  time_end   <- paste0(as.Date(Sys.Date()), "00:00:00")
  time_start <- paste0(as.Date(Sys.Date() - 365), "00:00:00")
  
  res <- azureDataConsumption(azureActiveContext=asc,
                              timeStart=time_start,
                              timeEnd=time_end,
                              granularity="Daily")
  
  expect_type(res, type="list")
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
  
  expect_type(res, type="list")
  expect_identical(object=names(res), expected=c("EffectiveDate", 
                                                 "IncludedQuantity",
                                                 "MeterCategory",
                                                 "MeterId",
                                                 "MeterName",
                                                 "MeterRegion",
                                                 "MeterStatus",
                                                 "MeterSubCategory",
                                                 "Unit",
                                                 "MeterRate"))
})


# total expense by day.

test_that("Get cost by day", {
  skip_if_missing_config(settingsfile)
  
  time_end   <- paste0(as.Date(Sys.Date()), "00:00:00")
  time_start <- paste0(as.Date(Sys.Date() - 365), "00:00:00")
  
  res <- azureExpenseCalculator(azureActiveContext=asc,
                                timeStart=time_start,
                                timeEnd=time_end,
                                granularity="Daily",
                                currency=config$CURRENCY,
                                locale=config$LOCALE,
                                offerId=config$OFFER,
                                region=config$REGION)
  
  expect_type(res, type="list")
  expect_identical(object=names(res), expected=c("MeterName",
                                                 "MeterCategory",
                                                 "MeterSubCategory",
                                                 "totalQuantity",
                                                 "Unit",
                                                 "MeterRate",
                                                 "Cost"))
})