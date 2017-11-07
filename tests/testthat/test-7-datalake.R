#' ------------------------------------------------------------------------
#' Sample config.json file for ADLS tests to run:
#' Create and place the file in the below specified location
#' ------------------------------------------------------------------------
#' 
#' {
#' "authType": "ClientCredential",
#' "resource": "https://datalake.azure.net/",
#' "tenantID": "72f988bf-blah-41af-blah-2d7cd011blah",
#' "clientID": "1d604733-blah-4b37-blah-98fca981blah",
#' "authKey": "zTw5blah+IN+yIblahrKv2K8dM2/BLah4FogBLAH/ME=",
#' "azureDataLakeAccount": "azuresmrtestadls"
#' }
#' 
#' ------------------------------------------------------------------------
#' NOTE:
#' ** authType can be one of "ClientCredential" (default), "DeviceCode" or "RefreshToken" (currently used internally by "DeviceCode" flow to refresh expired access tokens using available refresh token).
#' ** authType = "DeviceCode" cannot be used in automated tests, since its a manual process.
#' ------------------------------------------------------------------------


if(interactive()) library("testthat")

settingsfile <- find_config_json()
config <- read.AzureSMR.config(settingsfile)

#  ------------------------------------------------------------------------

context("Data Lake Store")

asc <- createAzureContext()
with(config,
     setAzureContext(asc, tenantID = tenantID, clientID = clientID, authKey = authKey, authType = authType, resource = resource)
)
azureAuthenticateOnAuthType(asc)

# NOTE: make sure to provide the azureDataLakeAccount name in the config file.
azureDataLakeAccount <- config$azureDataLakeAccount

context(" - data lake store")
test_that("Can create, list, get, update and delete items in an azure data lake account", {
  skip_if_missing_config(settingsfile)

  # cleanup the account before starting tests!
  try(
    azureDataLakeDelete(asc, azureDataLakeAccount, "tempfolder", TRUE)
  )

  # now start the tests

  # LISTSTATUS on non-existent test directory
  expect_error(azureDataLakeListStatus(asc, azureDataLakeAccount, "tempfolder"))
  # GETFILESTATUS on non-existent test directory
  expect_error(azureDataLakeGetFileStatus(asc, azureDataLakeAccount, "tempfolder"))

  # MKDIRS
  res <- azureDataLakeMkdirs(asc, azureDataLakeAccount, "tempfolder")
  expect_true(res)
  # MKDIRS - check 1 - LISTSTATUS
  res <- azureDataLakeListStatus(asc, azureDataLakeAccount, "")
  expect_is(res, "data.frame")
  expect_equal(nrow(res), 1)
  expect_equal(ncol(res), 11)
  # pathsuffix of a file/directory in liststatus will NOT be empty!
  expect_equal(res$FileStatuses.FileStatus.pathSuffix[1] == "tempfolder", TRUE)
  # MKDIRS - check 2 - GETFILESTATUS
  res <- azureDataLakeGetFileStatus(asc, azureDataLakeAccount, "")
  expect_is(res, "data.frame")
  expect_equal(nrow(res), 1)
  expect_equal(ncol(res), 11)
  # pathsuffix of a directory in getfilestatus will be empty!
  expect_equal(res$FileStatus.pathSuffix == "", TRUE)

  # CREATE
  res <- azureDataLakeCreate(asc, azureDataLakeAccount, "tempfolder/tempfile00.txt", "755", FALSE, 4194304L, 3L, 268435456L, charToRaw("abcd"))
  expect_null(res)
  res <- azureDataLakeCreate(asc, azureDataLakeAccount, "tempfolder/tempfile01.txt", "755", FALSE, 4194304L, 3L, 268435456L, charToRaw("efgh"))
  expect_null(res)
  # CREATE - check
  res <- azureDataLakeListStatus(asc, azureDataLakeAccount, "tempfolder")
  expect_is(res, "data.frame")
  expect_equal(nrow(res), 2)
  expect_equal(ncol(res), 12)
  expect_equal(res$FileStatuses.FileStatus.pathSuffix, c("tempfile00.txt", "tempfile01.txt"))
  expect_equal(res$FileStatuses.FileStatus.length, c(4, 4))
  # CREATE - check 2
  res <- azureDataLakeGetFileStatus(asc, azureDataLakeAccount, "tempfolder/tempfile00.txt")
  expect_is(res, "data.frame")
  expect_equal(nrow(res), 1)
  expect_equal(ncol(res), 12)
  # pathsuffix of a file in getfilestatus will be empty!
  expect_equal(res$FileStatus.pathSuffix == "", TRUE)
  expect_equal(res$FileStatus.length, 4)

  # READ (OPEN) # CREATE - check 3
  res <- azureDataLakeRead(asc, azureDataLakeAccount, "tempfolder/tempfile00.txt", length = 2L, bufferSize = 4194304L)
  expect_equal(rawToChar(res), "ab")
  res <- azureDataLakeRead(asc, azureDataLakeAccount, "tempfolder/tempfile01.txt", 2L, 2L, 4194304L)
  expect_equal(rawToChar(res), "gh")

  # APPEND
  res <- azureDataLakeAppend(asc, azureDataLakeAccount, "tempfolder/tempfile00.txt", 4194304L, charToRaw("stuv"))
  expect_null(res)
  res <- azureDataLakeAppend(asc, azureDataLakeAccount, "tempfolder/tempfile01.txt", 4194304L, charToRaw("wxyz"))
  expect_null(res)
  # APPEND - check
  res <- azureDataLakeListStatus(asc, azureDataLakeAccount, "tempfolder")
  expect_is(res, "data.frame")
  expect_equal(nrow(res), 2)
  expect_equal(ncol(res), 12)
  expect_equal(res$FileStatuses.FileStatus.pathSuffix, c("tempfile00.txt", "tempfile01.txt"))
  expect_equal(res$FileStatuses.FileStatus.length, c(8, 8))
  # APPEND - check 2 - READ (OPEN)
  res <- azureDataLakeRead(asc, azureDataLakeAccount, "tempfolder/tempfile00.txt")
  expect_equal(rawToChar(res), "abcdstuv")
  res <- azureDataLakeRead(asc, azureDataLakeAccount, "tempfolder/tempfile01.txt")
  expect_equal(rawToChar(res), "efghwxyz")

  # DELETE
  res <- azureDataLakeDelete(asc, azureDataLakeAccount, "tempfolder", TRUE)
  expect_true(res)
  # DELETE - check
  res <- azureDataLakeListStatus(asc, azureDataLakeAccount, "")
  expect_is(res, "data.frame")
  expect_equal(nrow(res), 0)
})

