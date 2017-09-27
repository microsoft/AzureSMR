if(interactive()) library("testthat")
settingsfile <- find_config_json()
config <- read.AzureSMR.config(settingsfile)

#  ------------------------------------------------------------------------

context("Data Lake Store")

asc <- createAzureContext()
with(config,
     setAzureContext(asc, tenantID = tenantID, clientID = clientID, authKey = authKey)
)
azureAuthenticate(asc)

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

  # LISTSTATUS on empty test directory
  expect_error(azureDataLakeListStatus(asc, azureDataLakeAccount, "tempfolder"))

  # MKDIRS
  res <- azureDataLakeMkdirs(asc, azureDataLakeAccount, "tempfolder")
  expect_true(res)
  # MKDIRS - check 1 - LISTSTATUS
  res <- azureDataLakeListStatus(asc, azureDataLakeAccount, "")
  expect_is(res, "data.frame")
  expect_equal(nrow(res), 1)
  # MKDIRS - check 2 - GETFILESTATUS
  res <- azureDataLakeGetFileStatus(asc, azureDataLakeAccount, "")
  expect_is(res, "data.frame")
  expect_equal(nrow(res), 1)

  # CREATE
  res <- azureDataLakeCreate(asc, azureDataLakeAccount, "tempfolder/tempfile00.txt", FALSE, "755", "abcd")
  expect_null(res)
  res <- azureDataLakeCreate(asc, azureDataLakeAccount, "tempfolder/tempfile01.txt", FALSE, "755", "efgh")
  expect_null(res)
  # CREATE - check
  res <- azureDataLakeListStatus(asc, azureDataLakeAccount, "tempfolder")
  expect_is(res, "data.frame")
  expect_equal(nrow(res), 2)
  expect_equal(res$FileStatuses.FileStatus.pathSuffix, c("tempfile00.txt", "tempfile01.txt"))
  expect_equal(res$FileStatuses.FileStatus.length, c(4, 4))

  # APPEND
  res <- azureDataLakeAppend(asc, azureDataLakeAccount, "tempfolder/tempfile00.txt", "stuv")
  expect_null(res)
  res <- azureDataLakeAppend(asc, azureDataLakeAccount, "tempfolder/tempfile01.txt", "wxyz")
  expect_null(res)
  # APPEND - check
  res <- azureDataLakeListStatus(asc, azureDataLakeAccount, "tempfolder")
  expect_is(res, "data.frame")
  expect_equal(nrow(res), 2)
  expect_equal(res$FileStatuses.FileStatus.pathSuffix, c("tempfile00.txt", "tempfile01.txt"))
  expect_equal(res$FileStatuses.FileStatus.length, c(8, 8))

  # DELETE
  res <- azureDataLakeDelete(asc, azureDataLakeAccount, "tempfolder", TRUE)
  expect_true(res)
  # DELETE - check
  res <- azureDataLakeListStatus(asc, azureDataLakeAccount, "")
  expect_is(res, "data.frame")
  expect_equal(nrow(res), 0)

})

