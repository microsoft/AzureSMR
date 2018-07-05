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
if(interactive()) library("mockery")

settingsfile <- find_config_json()
config <- read.AzureSMR.config(settingsfile)

context("Data Lake Store")

# Initialize azureActiveContext  ----

asc <- createAzureContext()
with(config,
     setAzureContext(asc, tenantID = tenantID, clientID = clientID, authKey = authKey, authType = authType, resource = resource)
)
azureAuthenticateOnAuthType(asc)

# NOTE: make sure to provide the azureDataLakeAccount name in the config file.
azureDataLakeAccount <- config$azureDataLakeAccount

# Test helper functions ----

createMockResponse <- function(httpRespStatusCode, httpRespContent) {
  # create an empty httr response object
  resp <- list()
  class(resp) <- "response"
  # populate the newly created httr response object with provided values
  if(!missing(httpRespStatusCode) && httpRespStatusCode >= 100) resp$status_code <- httpRespStatusCode
  if(!missing(httpRespContent) && !is.null(httpRespContent) && nchar(httpRespContent) > 0) {
    resp$content <- httpRespContent
    resp$headers[["Content-Type"]] <- "application/json; charset=utf-8"
  }
  return(resp)
}

# test_that("Can create, list, get, update and delete items in an azure data lake account", { ----

test_that("Can create, list, get, update and delete items in an azure data lake account", {
  skip_if_missing_config(settingsfile)

  printADLSMessage("test-7-datalake.R", "test_that",
                   "Can create, list, get, update and delete items in an azure data lake account",
                   NULL)

  verbose <- FALSE

  testFolder <- "tempfolder1?1文件夹1" # also test for special characters and utf16 languages
  Encoding(testFolder) <- "UTF-8" # need to explicitly set the string encoding in case of other language characters!?
  testFile1 <- "tempfile00.txt"
  testFile2 <- "tempfile01.txt"

  # cleanup the account before starting tests!
  try(
    azureDataLakeDelete(asc, azureDataLakeAccount, testFolder, TRUE, verbose = verbose)
  )

  # now start the tests

  # LISTSTATUS on non-existent test directory
  expect_error(azureDataLakeListStatus(asc, azureDataLakeAccount, testFolder))
  # GETFILESTATUS on non-existent test directory
  expect_error(azureDataLakeGetFileStatus(asc, azureDataLakeAccount, testFolder, verbose))

  # MKDIRS
  res <- azureDataLakeMkdirs(asc, azureDataLakeAccount, testFolder, verbose = verbose)
  expect_true(res)
  # MKDIRS - check 1 - LISTSTATUS
  res <- azureDataLakeListStatus(asc, azureDataLakeAccount, "", verbose = verbose)
  expect_is(res, "data.frame")
  expect_gte(nrow(res), 1)
  expect_equal(ncol(res), 11)
  # pathsuffix of a file/directory in liststatus will NOT be empty!
  expect_true(!is.null(res$FileStatuses.FileStatus.pathSuffix[1]))
  expect_true(nchar(res$FileStatuses.FileStatus.pathSuffix[1]) > 1)
  #expect_equal(res$FileStatuses.FileStatus.pathSuffix[1] == testFolder, TRUE)
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

# test_that("Can append and read using buffered IO streams from files in an azure data lake account", { ----

datafile2MB <- paste0(getwd(), "/data/test2MB.bin")
datafile4MB <- paste0(getwd(), "/data/test4MB.bin")
datafile6MB <- paste0(getwd(), "/data/test6MB.bin")

test_that("Can append and read using buffered IO streams from files in an azure data lake account", {
  skip_if_missing_config(settingsfile)

  printADLSMessage("test-7-datalake.R", "test_that",
                   "Can append and read using buffered IO streams from files in an azure data lake account",
                   NULL)

  verbose <- FALSE

  # cleanup the account before starting tests!
  try(
    azureDataLakeDelete(asc, azureDataLakeAccount, "tempfolder1", TRUE, verbose = verbose)
  )

  # MKDIRS
  res <- azureDataLakeMkdirs(asc, azureDataLakeAccount, "tempfolder1", verbose = verbose)
  expect_true(res)

  # CREATE
  res <- azureDataLakeCreate(asc, azureDataLakeAccount, "tempfolder1/test2MB.bin", "755", verbose = verbose)
  expect_null(res)
  # APPEND - test2MB.bin
  binData <- readBin(con = datafile2MB, what = "raw", n = 2097152)
  adlFOS <- azureDataLakeAppendBOS(asc, azureDataLakeAccount, "tempfolder1/test2MB.bin", verbose = verbose)
  expect_is(adlFOS, "adlFileOutputStream")
  res <- adlFileOutputStreamWrite(adlFOS, binData, 1, 2097152L, verbose = verbose)
  expect_null(res)
  res <- adlFileOutputStreamClose(adlFOS)
  expect_null(res)
  # APPEND - test2MB.bin - check
  res <- azureDataLakeGetFileStatus(asc, azureDataLakeAccount, "tempfolder1/test2MB.bin", verbose = verbose)
  expect_is(res, "data.frame")
  expect_equal(nrow(res), 1)
  expect_equal(ncol(res), 12)
  # pathsuffix of a file in getfilestatus will be empty!
  expect_equal(res$FileStatus.pathSuffix == "", TRUE)
  expect_equal(res$FileStatus.length, 2097152)
  # OPEN(READ) - test2MB.bin
  adlFIS <- azureDataLakeOpenBIS(asc, azureDataLakeAccount, "tempfolder1/test2MB.bin", verbose = verbose)
  buffer <- raw(2097152)
  res <- adlFileInputStreamRead(adlFIS, 0L, buffer, 1L, 2097152L, verbose = verbose)
  expect_equal(res[[1]], 2097152)
  expect_equal(res[[2]], binData)
  res <- adlFileInputStreamClose(adlFIS, verbose = verbose)
  # OPEN(READ_BUFFERED) - test2MB.bin
  adlFIS <- azureDataLakeOpenBIS(asc, azureDataLakeAccount, "tempfolder1/test2MB.bin")
  buffer <- raw(2097152)
  res <- adlFileInputStreamReadBuffered(adlFIS, buffer, 1L, 2097152L, verbose = verbose)
  expect_equal(res[[1]], 2097152)
  expect_equal(res[[2]], binData)
  res <- adlFileInputStreamClose(adlFIS, verbose = verbose)
  # OPEN(SEEK, READ_BUFFERED) - test2MB.bin
  adlFIS <- azureDataLakeOpenBIS(asc, azureDataLakeAccount, "tempfolder1/test2MB.bin", verbose = verbose)
  res <- adlFileInputStreamSeek(adlFIS, 1048576)
  expect_null(res)
  res <- adlFileInputStreamGetPos(adlFIS)
  expect_equal(res, 1048576)
  buffer <- raw(1048576)
  res <- adlFileInputStreamReadBuffered(adlFIS, buffer, 1L, 1048576L, verbose = verbose)
  expect_equal(res[[1]], 1048576)
  expect_equal(res[[2]], binData[1048577:2097152])
  res <- adlFileInputStreamClose(adlFIS, verbose = verbose)
  # OPEN(SKIP, READ_BUFFERED) - test2MB.bin
  adlFIS <- azureDataLakeOpenBIS(asc, azureDataLakeAccount, "tempfolder1/test2MB.bin", verbose = verbose)
  res <- adlFileInputStreamSkip(adlFIS, 1048576)
  expect_equal(res, 1048576)
  adlFileInputStreamGetPos(adlFIS)
  buffer <- raw(1048576)
  res <- adlFileInputStreamReadBuffered(adlFIS, buffer, 1L, 1048576L, verbose = verbose)
  expect_equal(res[[1]], 1048576)
  expect_equal(res[[2]], binData[1048577:2097152])
  res <- adlFileInputStreamClose(adlFIS, verbose = verbose)

  # CREATE
  res <- azureDataLakeCreate(asc, azureDataLakeAccount, "tempfolder1/test4MB.bin", "755", verbose = verbose)
  expect_null(res)
  # APPEND - test4MB.bin
  binData <- readBin(con = datafile4MB, what = "raw", n = 4194304)
  adlFOS <- azureDataLakeAppendBOS(asc, azureDataLakeAccount, "tempfolder1/test4MB.bin", verbose = verbose)
  expect_is(adlFOS, "adlFileOutputStream")
  res <- adlFileOutputStreamWrite(adlFOS, binData, 1, 4194304L, verbose = verbose)
  expect_null(res)
  res <- adlFileOutputStreamClose(adlFOS, verbose = verbose)
  expect_null(res)
  # APPEND - test4MB.bin - check
  res <- azureDataLakeGetFileStatus(asc, azureDataLakeAccount, "tempfolder1/test4MB.bin", verbose = verbose)
  expect_is(res, "data.frame")
  expect_equal(nrow(res), 1)
  expect_equal(ncol(res), 12)
  # pathsuffix of a file in getfilestatus will be empty!
  expect_equal(res$FileStatus.pathSuffix == "", TRUE)
  expect_equal(res$FileStatus.length, 4194304)
  # OPEN(READ) - test4MB.bin
  adlFIS <- azureDataLakeOpenBIS(asc, azureDataLakeAccount, "tempfolder1/test4MB.bin", verbose = verbose)
  buffer <- raw(4194304)
  res <- adlFileInputStreamRead(adlFIS, 0L, buffer, 1L, 4194304L, verbose = verbose)
  expect_equal(res[[1]], 4194304)
  expect_equal(res[[2]], binData)
  res <- adlFileInputStreamClose(adlFIS, verbose = verbose)
  # OPEN(READ_BUFFERED) - test4MB.bin
  adlFIS <- azureDataLakeOpenBIS(asc, azureDataLakeAccount, "tempfolder1/test4MB.bin", verbose = verbose)
  buffer <- raw(4194304)
  res <- adlFileInputStreamReadBuffered(adlFIS, buffer, 1L, 4194304L, verbose = verbose)
  expect_equal(res[[1]], 4194304)
  expect_equal(res[[2]], binData)
  res <- adlFileInputStreamClose(adlFIS, verbose = verbose)

  # CREATE
  res <- azureDataLakeCreate(asc, azureDataLakeAccount, "tempfolder1/test6MB.bin", "755", verbose = verbose)
  expect_null(res)
  # APPEND - test6MB.bin
  binData <- readBin(con = datafile6MB, what = "raw", n = 6291456)
  adlFOS <- azureDataLakeAppendBOS(asc, azureDataLakeAccount, "tempfolder1/test6MB.bin", verbose = verbose)
  expect_is(adlFOS, "adlFileOutputStream")
  res <- adlFileOutputStreamWrite(adlFOS, binData, 1, 6291456L, verbose = verbose)
  expect_null(res)
  res <- adlFileOutputStreamClose(adlFOS, verbose = verbose)
  expect_null(res)
  # APPEND - test6MB.bin - check
  res <- azureDataLakeGetFileStatus(asc, azureDataLakeAccount, "tempfolder1/test6MB.bin", verbose = verbose)
  expect_is(res, "data.frame")
  expect_equal(nrow(res), 1)
  expect_equal(ncol(res), 12)
  # pathsuffix of a file in getfilestatus will be empty!
  expect_equal(res$FileStatus.pathSuffix == "", TRUE)
  expect_equal(res$FileStatus.length, 6291456)
  # OPEN(READ) - test6MB.bin
  adlFIS <- azureDataLakeOpenBIS(asc, azureDataLakeAccount, "tempfolder1/test6MB.bin", verbose = verbose)
  buffer <- raw(6291456)
  res <- adlFileInputStreamRead(adlFIS, 0L, buffer, 1L, 6291456L, verbose = verbose)
  expect_equal(res[[1]], 6291456)
  expect_equal(res[[2]], binData)
  res <- adlFileInputStreamClose(adlFIS, verbose = verbose)
  # OPEN(READ_BUFFERED) - test6MB.bin
  adlFIS <- azureDataLakeOpenBIS(asc, azureDataLakeAccount, "tempfolder1/test6MB.bin", verbose = verbose)
  buffer <- raw(6291456)
  res <- adlFileInputStreamReadBuffered(adlFIS, buffer, 1L, 6291456L, verbose = verbose)
  expect_equal(res[[1]], 4194304)
  expect_equal(res[[2]][1:4194304], binData[1:4194304])
  res <- adlFileInputStreamClose(adlFIS, verbose = verbose)

  # CREATE
  res <- azureDataLakeCreate(asc, azureDataLakeAccount, "tempfolder1/test6MBWA.bin", "755", verbose = verbose)
  expect_null(res)
  # **** APPEND - 2MB to test6MBWA.bin
  binData <- readBin(con = datafile2MB, what = "raw", n = 2097152)
  binDataWA <- binData
  adlFOS <- azureDataLakeAppendBOS(asc, azureDataLakeAccount, "tempfolder1/test6MBWA.bin", verbose = verbose)
  expect_is(adlFOS, "adlFileOutputStream")
  res <- adlFileOutputStreamWrite(adlFOS, binData, 1, 2097152L, verbose = verbose)
  expect_null(res)
  res <- adlFileOutputStreamClose(adlFOS, verbose = verbose)
  expect_null(res)
  # CHECK - APPEND - 2MB to test6MBWA.bin
  res <- azureDataLakeGetFileStatus(asc, azureDataLakeAccount, "tempfolder1/test6MBWA.bin", verbose = verbose)
  expect_is(res, "data.frame")
  expect_equal(nrow(res), 1)
  expect_equal(ncol(res), 12)
  # pathsuffix of a file in getfilestatus will be empty!
  expect_equal(res$FileStatus.pathSuffix == "", TRUE)
  expect_equal(res$FileStatus.length, 2097152)
  # **** APPEND - 4MB to test6MBWA.bin
  binData <- readBin(con = datafile4MB, what = "raw", n = 4194304)
  binDataWA[2097153:6291456] <- binData[1:4194304]
  adlFOS <- azureDataLakeAppendBOS(asc, azureDataLakeAccount, "tempfolder1/test6MBWA.bin", verbose = verbose)
  expect_is(adlFOS, "adlFileOutputStream")
  res <- adlFileOutputStreamWrite(adlFOS, binData, 1, 4194304L, verbose = verbose)
  expect_null(res)
  res <- adlFileOutputStreamClose(adlFOS, verbose = verbose)
  expect_null(res)
  # CHECK - APPEND - 4MB to test6MBWA.bin
  res <- azureDataLakeGetFileStatus(asc, azureDataLakeAccount, "tempfolder1/test6MBWA.bin", verbose = verbose)
  expect_is(res, "data.frame")
  expect_equal(nrow(res), 1)
  expect_equal(ncol(res), 12)
  # pathsuffix of a file in getfilestatus will be empty!
  expect_equal(res$FileStatus.pathSuffix == "", TRUE)
  expect_equal(res$FileStatus.length, 6291456)
  # OPEN(READ) - test6MBWA.bin
  adlFIS <- azureDataLakeOpenBIS(asc, azureDataLakeAccount, "tempfolder1/test6MBWA.bin", verbose = verbose)
  buffer <- raw(6291456)
  res <- adlFileInputStreamRead(adlFIS, 0L, buffer, 1L, 6291456L, verbose = verbose)
  expect_equal(res[[1]], 6291456)
  expect_equal(res[[2]], binDataWA)
  res <- adlFileInputStreamClose(adlFIS, verbose = verbose)
  # OPEN(READ) - test6MBWA.bin- check with offset
  adlFIS <- azureDataLakeOpenBIS(asc, azureDataLakeAccount, "tempfolder1/test6MBWA.bin", verbose = verbose)
  buffer <- raw(6291456)
  res <- adlFileInputStreamRead(adlFIS, 2097152L, buffer, 2097153L, 4194304L, verbose = verbose)
  expect_equal(res[[1]], 4194304)
  expect_equal(res[[2]][2097153:6291456], binData)
  res <- adlFileInputStreamClose(adlFIS, verbose = verbose)
  # OPEN(READ_BUFFERED) - test6MBWA.bin
  adlFIS <- azureDataLakeOpenBIS(asc, azureDataLakeAccount, "tempfolder1/test6MBWA.bin", verbose = verbose)
  buffer <- raw(6291456)
  res <- adlFileInputStreamReadBuffered(adlFIS, buffer, 1L, 6291456L, verbose = verbose)
  expect_equal(res[[1]], 4194304)
  expect_equal(res[[2]][1:4194304], binDataWA[1:4194304])
  res <- adlFileInputStreamClose(adlFIS, verbose = verbose)

  # DELETE
  res <- azureDataLakeDelete(asc, azureDataLakeAccount, "tempfolder1", TRUE, verbose = verbose)
  expect_true(res)
})

# test_that("Retries in R SDK for azure data lake account", { ----

test_that("Retries in R SDK for azure data lake account", {
  skip_if_missing_config(settingsfile)

  printADLSMessage("test-7-datalake.R", "test_that",
                   "Retries in R SDK for azure data lake account",
                   NULL)

  verbose <- FALSE

  testFolder <- "tempfolder1Retries"

  # MKDIR - mock with 4 fail and 5th success response.
  # This should PASS since the last(4th) retry succeeded.
  mockCallAzureDataLakeRestEndPoint <- mock(
    createMockResponse(500), # initial call - mock error response
    createMockResponse(408), # retry - 1 - mock error response
    createMockResponse(429), # retry - 2 - mock error response
    createMockResponse(401), # retry - 3 - mock error response
    createMockResponse(200,  # retry - 4 - last retry - mock success response - overall PASS
                   charToRaw("{\"boolean\":true}")),
    cycle = FALSE)
  # NOTE: Stubbing is not a good idea as it may create complications for the rest of the tests.
  # Moreover it doesnt seem to be working as expected in R!
  # Instead use with_mock().
  #stub(azureDataLakeMkdirs, 'callAzureDataLakeRestEndPoint', mockCallAzureDataLakeRestEndPoint, depth = 2)
  with_mock(
    # use fully qualified name of function to be mocked, else this doesnt work
    "AzureSMR:::callAzureDataLakeRestEndPoint" = mockCallAzureDataLakeRestEndPoint,
    res <- azureDataLakeMkdirs(asc, azureDataLakeAccount, testFolder, verbose = verbose),
    expect_called(mockCallAzureDataLakeRestEndPoint, 5)
  )
  expect_true(res)

  # MKDIR - mock with 5 fail and 6th success response.
  # This should FAIL since the last(4th) retry failed.
  mockCallAzureDataLakeRestEndPoint <- mock(
    createMockResponse(500), # initial call - mock error response
    createMockResponse(500), # retry - 1 - mock error response
    createMockResponse(500), # retry - 2 - mock error response
    createMockResponse(500), # retry - 3 - mock error response
    createMockResponse(500,  # retry - 4 - last retry - mock error response - overall FAIL
                   charToRaw("{\"RemoteException\":{\"exception\":\"RuntimeException\",\"message\":\"MkDir failed with error xxx\",\"javaClassName\":\"java.lang.RuntimeException\"}}")),
    createMockResponse(200,  # retry - 5 - retry limit exceeded - mock success response - retries finished, so overall FAIL
                   charToRaw("{\"boolean\":true}")), 
    cycle = FALSE)
  with_mock(
    "AzureSMR:::callAzureDataLakeRestEndPoint" = mockCallAzureDataLakeRestEndPoint,
    expect_error(azureDataLakeMkdirs(asc, azureDataLakeAccount, testFolder, verbose = verbose)),
    expect_called(mockCallAzureDataLakeRestEndPoint, 5)
  )

  # GETFILESTATUS - mock with 1 HTTP404 and then success response.
  # Since first error is non-retriable, overall request should still FAIL.
  mockCallAzureDataLakeRestEndPoint <- mock(
    createMockResponse(404), # initial call - mock error response
    createMockResponse(200,  # retry - 1 - mock success response
                   charToRaw(paste0("{\"FileStatus\":"
                                    , "{"
                                    , "\"length\":0"
                                    , ",\"pathSuffix\":\"\""
                                    , ",\"type\":\"DIRECTORY\""
                                    , ",\"blockSize\":0"
                                    , ",\"accessTime\":1527951297440"
                                    , ",\"modificationTime\":1528753497003"
                                    , ",\"replication\":0"
                                    , ",\"permission\":\"770\""
                                    , ",\"owner\":\"4b27fe1a-blah-4a04-blah-4bbaBLAH9e6c\""
                                    , ",\"group\":\"4b27fe1a-blah-4a04-blah-4bbaBLAH9e6c\""
                                    , "}"
                                    , "}"
                                    )
                             ),
                   cycle = FALSE)
    )
  with_mock(
    "AzureSMR:::callAzureDataLakeRestEndPoint" = mockCallAzureDataLakeRestEndPoint,
    expect_error(azureDataLakeGetFileStatus(asc, azureDataLakeAccount, testFolder, verbose = verbose)),
    expect_called(mockCallAzureDataLakeRestEndPoint, 1)
  )
  
  # CREATE (overwrite = TRUE) - mock with 2 fail and 3rd success response.
  # This will be an ExponentialBackoffRetryPolicy. 
  # This should PASS because the 2nd retry succeeded.
  mockCallAzureDataLakeRestEndPoint <- mock(
    createMockResponse(500), # initial call - mock error response
    createMockResponse(408), # retry - 1 - mock error response
    createMockResponse(200), # retry - 2 - mock success response - overall PASS
    cycle = FALSE)
  with_mock(
    # use fully qualified name of function to be mocked, else this doesnt work
    "AzureSMR:::callAzureDataLakeRestEndPoint" = mockCallAzureDataLakeRestEndPoint,
    res <- azureDataLakeCreate(asc, azureDataLakeAccount, testFolder, overwrite = TRUE, verbose = verbose),
    expect_called(mockCallAzureDataLakeRestEndPoint, 3)
  )
  expect_null(res)
  
  # CREATE (overwrite = FALSE) - mock with 2 fail and 3rd success response.
  # This will be NonIdempotentRetryPolicy. 
  # This should FAIL because the 2rd retry in non-retriable.
  mockCallAzureDataLakeRestEndPoint <- mock(
    createMockResponse(401), # initial call - mock error response
    createMockResponse(429), # retry - 1 - mock error response
    createMockResponse(500), # retry - 2 - mock success response - overall PASS
    cycle = FALSE)
  with_mock(
    # use fully qualified name of function to be mocked, else this doesnt work
    "AzureSMR:::callAzureDataLakeRestEndPoint" = mockCallAzureDataLakeRestEndPoint,
    expect_error(azureDataLakeCreate(asc, azureDataLakeAccount, testFolder, overwrite = FALSE, verbose = verbose)),
    expect_called(mockCallAzureDataLakeRestEndPoint, 3)
  )
})
