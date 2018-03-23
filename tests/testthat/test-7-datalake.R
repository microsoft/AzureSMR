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

  testFolder <- "tempfolder1?1文件夹1" # also test for special characters and utf16 languages
  Encoding(testFolder) <- "UTF-8" # need to explicitly set the string encoding in case of other language characters!?
  testFile1 <- "tempfile00.txt"
  testFile2 <- "tempfile01.txt"

  verbose <- FALSE

  # cleanup the account before starting tests!
  try(
    azureDataLakeDelete(asc, azureDataLakeAccount, testFolder, TRUE, verbose = verbose)
  )

  # now start the tests

  # LISTSTATUS on non-existent test directory
  expect_error(azureDataLakeListStatus(asc, azureDataLakeAccount, testFolder))
  # GETFILESTATUS on non-existent test directory
  expect_error(azureDataLakeGetFileStatus(asc, azureDataLakeAccount, testFolder))

  # MKDIRS
  res <- azureDataLakeMkdirs(asc, azureDataLakeAccount, testFolder, verbose = verbose)
  expect_true(res)
  # MKDIRS - check 1 - LISTSTATUS
  res <- azureDataLakeListStatus(asc, azureDataLakeAccount, "", verbose = verbose)
  expect_is(res, "data.frame")
  expect_gte(nrow(res), 1)
  expect_equal(ncol(res), 11)
  # pathsuffix of a file/directory in liststatus will NOT be empty!
  expect_equal(res$FileStatuses.FileStatus.pathSuffix[1] == testFolder, TRUE)
  # MKDIRS - check 2 - GETFILESTATUS
  res <- azureDataLakeGetFileStatus(asc, azureDataLakeAccount, "", verbose = verbose)
  expect_is(res, "data.frame")
  expect_gte(nrow(res), 1)
  expect_equal(ncol(res), 11)
  # pathsuffix of a directory in getfilestatus will be empty!
  expect_equal(res$FileStatus.pathSuffix == "", TRUE)

  # CREATE
  res <- azureDataLakeCreate(asc, azureDataLakeAccount, paste0(testFolder, "/", testFile1), "755", FALSE, 4194304L, 3L, 268435456L, charToRaw("abcd"), verbose = verbose)
  expect_null(res)
  res <- azureDataLakeCreate(asc, azureDataLakeAccount, paste0(testFolder, "/", testFile2), "755", FALSE, 4194304L, 3L, 268435456L, charToRaw("efgh"), verbose = verbose)
  expect_null(res)
  # CREATE - check
  res <- azureDataLakeListStatus(asc, azureDataLakeAccount, testFolder, verbose = verbose)
  expect_is(res, "data.frame")
  expect_equal(nrow(res), 2)
  expect_equal(ncol(res), 12)
  expect_equal(res$FileStatuses.FileStatus.pathSuffix, c(testFile1, testFile2))
  expect_equal(res$FileStatuses.FileStatus.length, c(4, 4))
  # CREATE - check 2
  res <- azureDataLakeGetFileStatus(asc, azureDataLakeAccount, paste0(testFolder, "/", testFile1), verbose = verbose)
  expect_is(res, "data.frame")
  expect_equal(nrow(res), 1)
  expect_equal(ncol(res), 12)
  # pathsuffix of a file in getfilestatus will be empty!
  expect_equal(res$FileStatus.pathSuffix == "", TRUE)
  expect_equal(res$FileStatus.length, 4)

  # READ (OPEN) # CREATE - check 3
  res <- azureDataLakeRead(asc, azureDataLakeAccount, paste0(testFolder, "/", testFile1), length = 2L, bufferSize = 4194304L, verbose = verbose)
  expect_equal(rawToChar(res), "ab")
  res <- azureDataLakeRead(asc, azureDataLakeAccount, paste0(testFolder, "/", testFile2), 2L, 2L, 4194304L, verbose = verbose)
  expect_equal(rawToChar(res), "gh")

  # APPEND
  res <- azureDataLakeAppend(asc, azureDataLakeAccount, paste0(testFolder, "/", testFile1), 4194304L, charToRaw("stuv"), verbose = verbose)
  expect_null(res)
  res <- azureDataLakeAppend(asc, azureDataLakeAccount, paste0(testFolder, "/", testFile2), 4194304L, charToRaw("wxyz"), verbose = verbose)
  expect_null(res)
  # APPEND - check
  res <- azureDataLakeListStatus(asc, azureDataLakeAccount, testFolder, verbose = verbose)
  expect_is(res, "data.frame")
  expect_equal(nrow(res), 2)
  expect_equal(ncol(res), 12)
  expect_equal(res$FileStatuses.FileStatus.pathSuffix, c(testFile1, testFile2))
  expect_equal(res$FileStatuses.FileStatus.length, c(8, 8))
  # APPEND - check 2 - READ (OPEN)
  res <- azureDataLakeRead(asc, azureDataLakeAccount, paste0(testFolder, "/", testFile1), verbose = verbose)
  expect_equal(rawToChar(res), "abcdstuv")
  res <- azureDataLakeRead(asc, azureDataLakeAccount, paste0(testFolder, "/", testFile2), verbose = verbose)
  expect_equal(rawToChar(res), "efghwxyz")

  # DELETE
  res <- azureDataLakeDelete(asc, azureDataLakeAccount, testFolder, TRUE, verbose = verbose)
  expect_true(res)
})

datafile2MB <- paste0(getwd(), "/data/test2MB.bin")
datafile4MB <- paste0(getwd(), "/data/test4MB.bin")
datafile6MB <- paste0(getwd(), "/data/test6MB.bin")

test_that("Can append and read using buffered IO streams from files in an azure data lake account", {
  skip_if_missing_config(settingsfile)

  # cleanup the account before starting tests!
  try(
    azureDataLakeDelete(asc, azureDataLakeAccount, "tempfolder1", TRUE)
  )

  # MKDIRS
  res <- azureDataLakeMkdirs(asc, azureDataLakeAccount, "tempfolder1")
  expect_true(res)

  # CREATE
  res <- azureDataLakeCreate(asc, azureDataLakeAccount, "tempfolder1/test2MB.bin", "755")
  expect_null(res)
  # APPEND - test2MB.bin
  print(datafile2MB)
  binData <- readBin(con = datafile2MB, what = "raw", n = 2097152)
  adlFOS <- azureDataLakeAppendBOS(asc, azureDataLakeAccount, "tempfolder1/test2MB.bin")
  expect_is(adlFOS, "adlFileOutputStream")
  res <- adlFileOutputStreamWrite(adlFOS, binData, 1, 2097152L)
  expect_null(res)
  res <- adlFileOutputStreamClose(adlFOS)
  expect_null(res)
  # APPEND - test2MB.bin - check
  res <- azureDataLakeGetFileStatus(asc, azureDataLakeAccount, "tempfolder1/test2MB.bin")
  expect_is(res, "data.frame")
  expect_equal(nrow(res), 1)
  expect_equal(ncol(res), 12)
  # pathsuffix of a file in getfilestatus will be empty!
  expect_equal(res$FileStatus.pathSuffix == "", TRUE)
  expect_equal(res$FileStatus.length, 2097152)
  # OPEN(READ) - test2MB.bin
  adlFIS <- azureDataLakeOpenBIS(asc, azureDataLakeAccount, "tempfolder1/test2MB.bin")
  buffer <- raw(2097152)
  res <- adlFileInputStreamRead(adlFIS, 0L, buffer, 1L, 2097152L, TRUE)
  expect_equal(res[[1]], 2097152)
  expect_equal(res[[2]], binData)
  res <- adlFileInputStreamClose(adlFIS, TRUE)
  # OPEN(READ_BUFFERED) - test2MB.bin
  adlFIS <- azureDataLakeOpenBIS(asc, azureDataLakeAccount, "tempfolder1/test2MB.bin")
  buffer <- raw(2097152)
  res <- adlFileInputStreamReadBuffered(adlFIS, buffer, 1L, 2097152L, TRUE)
  expect_equal(res[[1]], 2097152)
  expect_equal(res[[2]], binData)
  res <- adlFileInputStreamClose(adlFIS, TRUE)

  # CREATE
  res <- azureDataLakeCreate(asc, azureDataLakeAccount, "tempfolder1/test4MB.bin", "755")
  expect_null(res)
  # APPEND - test4MB.bin
  binData <- readBin(con = datafile4MB, what = "raw", n = 4194304)
  adlFOS <- azureDataLakeAppendBOS(asc, azureDataLakeAccount, "tempfolder1/test4MB.bin")
  expect_is(adlFOS, "adlFileOutputStream")
  res <- adlFileOutputStreamWrite(adlFOS, binData, 1, 4194304L)
  expect_null(res)
  res <- adlFileOutputStreamClose(adlFOS)
  expect_null(res)
  # APPEND - test4MB.bin - check
  res <- azureDataLakeGetFileStatus(asc, azureDataLakeAccount, "tempfolder1/test4MB.bin")
  expect_is(res, "data.frame")
  expect_equal(nrow(res), 1)
  expect_equal(ncol(res), 12)
  # pathsuffix of a file in getfilestatus will be empty!
  expect_equal(res$FileStatus.pathSuffix == "", TRUE)
  expect_equal(res$FileStatus.length, 4194304)
  # OPEN(READ) - test4MB.bin
  adlFIS <- azureDataLakeOpenBIS(asc, azureDataLakeAccount, "tempfolder1/test4MB.bin")
  buffer <- raw(4194304)
  res <- adlFileInputStreamRead(adlFIS, 0L, buffer, 1L, 4194304L, TRUE)
  expect_equal(res[[1]], 4194304)
  expect_equal(res[[2]], binData)
  res <- adlFileInputStreamClose(adlFIS, TRUE)
  # OPEN(READ_BUFFERED) - test4MB.bin
  adlFIS <- azureDataLakeOpenBIS(asc, azureDataLakeAccount, "tempfolder1/test4MB.bin")
  buffer <- raw(4194304)
  res <- adlFileInputStreamReadBuffered(adlFIS, buffer, 1L, 4194304L, TRUE)
  expect_equal(res[[1]], 4194304)
  expect_equal(res[[2]], binData)
  res <- adlFileInputStreamClose(adlFIS, TRUE)

  # CREATE
  res <- azureDataLakeCreate(asc, azureDataLakeAccount, "tempfolder1/test6MB.bin", "755")
  expect_null(res)
  # APPEND - test6MB.bin
  binData <- readBin(con = datafile6MB, what = "raw", n = 6291456)
  adlFOS <- azureDataLakeAppendBOS(asc, azureDataLakeAccount, "tempfolder1/test6MB.bin")
  expect_is(adlFOS, "adlFileOutputStream")
  res <- adlFileOutputStreamWrite(adlFOS, binData, 1, 6291456L)
  expect_null(res)
  res <- adlFileOutputStreamClose(adlFOS)
  expect_null(res)
  # APPEND - test6MB.bin - check
  res <- azureDataLakeGetFileStatus(asc, azureDataLakeAccount, "tempfolder1/test6MB.bin")
  expect_is(res, "data.frame")
  expect_equal(nrow(res), 1)
  expect_equal(ncol(res), 12)
  # pathsuffix of a file in getfilestatus will be empty!
  expect_equal(res$FileStatus.pathSuffix == "", TRUE)
  expect_equal(res$FileStatus.length, 6291456)
  # OPEN(READ) - test6MB.bin
  adlFIS <- azureDataLakeOpenBIS(asc, azureDataLakeAccount, "tempfolder1/test6MB.bin")
  buffer <- raw(6291456)
  res <- adlFileInputStreamRead(adlFIS, 0L, buffer, 1L, 6291456L, TRUE)
  expect_equal(res[[1]], 6291456)
  expect_equal(res[[2]], binData)
  res <- adlFileInputStreamClose(adlFIS, TRUE)
  # OPEN(READ_BUFFERED) - test6MB.bin
  adlFIS <- azureDataLakeOpenBIS(asc, azureDataLakeAccount, "tempfolder1/test6MB.bin")
  buffer <- raw(6291456)
  res <- adlFileInputStreamReadBuffered(adlFIS, buffer, 1L, 6291456L, TRUE)
  expect_equal(res[[1]], 4194304)
  expect_equal(res[[2]][1:4194304], binData[1:4194304])
  res <- adlFileInputStreamClose(adlFIS, TRUE)

  # CREATE
  res <- azureDataLakeCreate(asc, azureDataLakeAccount, "tempfolder1/test6MBWA.bin", "755")
  expect_null(res)
  # **** APPEND - 2MB to test6MBWA.bin
  binData <- readBin(con = datafile2MB, what = "raw", n = 2097152)
  binDataWA <- binData
  adlFOS <- azureDataLakeAppendBOS(asc, azureDataLakeAccount, "tempfolder1/test6MBWA.bin")
  expect_is(adlFOS, "adlFileOutputStream")
  res <- adlFileOutputStreamWrite(adlFOS, binData, 1, 2097152L)
  expect_null(res)
  res <- adlFileOutputStreamClose(adlFOS)
  expect_null(res)
  # CHECK - APPEND - 2MB to test6MBWA.bin
  res <- azureDataLakeGetFileStatus(asc, azureDataLakeAccount, "tempfolder1/test6MBWA.bin")
  expect_is(res, "data.frame")
  expect_equal(nrow(res), 1)
  expect_equal(ncol(res), 12)
  # pathsuffix of a file in getfilestatus will be empty!
  expect_equal(res$FileStatus.pathSuffix == "", TRUE)
  expect_equal(res$FileStatus.length, 2097152)
  # **** APPEND - 4MB to test6MBWA.bin
  binData <- readBin(con = datafile4MB, what = "raw", n = 4194304)
  binDataWA[2097153:6291456] <- binData[1:4194304]
  adlFOS <- azureDataLakeAppendBOS(asc, azureDataLakeAccount, "tempfolder1/test6MBWA.bin")
  expect_is(adlFOS, "adlFileOutputStream")
  res <- adlFileOutputStreamWrite(adlFOS, binData, 1, 4194304L)
  expect_null(res)
  res <- adlFileOutputStreamClose(adlFOS)
  expect_null(res)
  # CHECK - APPEND - 4MB to test6MBWA.bin
  res <- azureDataLakeGetFileStatus(asc, azureDataLakeAccount, "tempfolder1/test6MBWA.bin")
  expect_is(res, "data.frame")
  expect_equal(nrow(res), 1)
  expect_equal(ncol(res), 12)
  # pathsuffix of a file in getfilestatus will be empty!
  expect_equal(res$FileStatus.pathSuffix == "", TRUE)
  expect_equal(res$FileStatus.length, 6291456)
  # OPEN(READ) - test6MBWA.bin
  adlFIS <- azureDataLakeOpenBIS(asc, azureDataLakeAccount, "tempfolder1/test6MBWA.bin")
  buffer <- raw(6291456)
  res <- adlFileInputStreamRead(adlFIS, 0L, buffer, 1L, 6291456L, TRUE)
  expect_equal(res[[1]], 6291456)
  expect_equal(res[[2]], binDataWA)
  res <- adlFileInputStreamClose(adlFIS, TRUE)
  # OPEN(READ) - test6MBWA.bin- check with offset
  adlFIS <- azureDataLakeOpenBIS(asc, azureDataLakeAccount, "tempfolder1/test6MBWA.bin")
  buffer <- raw(6291456)
  res <- adlFileInputStreamRead(adlFIS, 2097152L, buffer, 2097153L, 4194304L, TRUE)
  expect_equal(res[[1]], 4194304)
  expect_equal(res[[2]][2097153:6291456], binData)
  res <- adlFileInputStreamClose(adlFIS, TRUE)
  # OPEN(READ_BUFFERED) - test6MBWA.bin
  adlFIS <- azureDataLakeOpenBIS(asc, azureDataLakeAccount, "tempfolder1/test6MBWA.bin")
  buffer <- raw(6291456)
  res <- adlFileInputStreamReadBuffered(adlFIS, buffer, 1L, 6291456L, TRUE)
  expect_equal(res[[1]], 4194304)
  expect_equal(res[[2]][1:4194304], binDataWA[1:4194304])
  res <- adlFileInputStreamClose(adlFIS, TRUE)

  # DELETE
  res <- azureDataLakeDelete(asc, azureDataLakeAccount, "tempfolder1", TRUE)
  expect_true(res)
})
