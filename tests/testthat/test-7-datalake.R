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

# test_that("Can create, list, get, update and delete items in R SDK for azure data lake account", { ----

test_that("Can create, list, get, update and delete items in R SDK for azure data lake account", {
  skip_if_missing_config(settingsfile)

  printADLSMessage("test-7-datalake.R", "test_that",
                   "Can create, list, get, update and delete items in R SDK for azure data lake account",
                   NULL)

  verbose <- FALSE

  testFolder <- "tempfolder1?1文件夹1" # also test for special characters and utf16 languages
  Encoding(testFolder) <- "UTF-8" # need to explicitly set the string encoding in case of other language characters!?
  testFile1 <- "tempfile01.txt"
  testFile2 <- "tempfile02.txt"
  testFolderRename <- "tempfolderRename"
  testFolderConcat <- testFolder #"tempFolderConcat"
  testFileConcatDest <- paste0(testFolderConcat, "/", "tempfileconcatdest.txt")
  testFileConcatSrc1 <- paste0(testFolderConcat, "/", "tempfileconcatsrc01.txt")
  testFileConcatSrc2 <- paste0(testFolderConcat, "/", "tempfileconcatsrc02.txt")
  testFileConcatSrc3 <- paste0(testFolderConcat, "/", "tempfileconcatsrc03.txt")

  # cleanup the account before starting tests!
  try(
    adls.delete(asc, azureDataLakeAccount, testFolder, TRUE, verbose = verbose)
  )
  try(
    adls.delete(asc, azureDataLakeAccount, testFolderRename, TRUE, verbose = verbose)
  )
  try(
    adls.delete(asc, azureDataLakeAccount, testFolderConcat, TRUE, verbose = verbose)
  )

  # now start the tests

  # LISTSTATUS on non-existent test directory
  expect_error(adls.ls(asc, azureDataLakeAccount, testFolder, verbose = verbose))
  # GETFILESTATUS on non-existent test directory
  expect_error(adls.file.info(asc, azureDataLakeAccount, testFolder, verbose = verbose))

  # MKDIRS
  res <- adls.mkdir(asc, azureDataLakeAccount, testFolder, verbose = verbose)
  expect_true(res)
  # MKDIRS - check 1 - LISTSTATUS
  res <- adls.ls(asc, azureDataLakeAccount, "", verbose = verbose)
  expect_is(res, "data.frame")
  expect_gte(nrow(res), 1)
  expect_equal(ncol(res), 11)
  # pathsuffix of a file/directory in liststatus will NOT be empty!
  expect_true(!is.null(res$FileStatuses.FileStatus.pathSuffix[1]))
  expect_true(nchar(res$FileStatuses.FileStatus.pathSuffix[1]) > 1)
  #expect_equal(res$FileStatuses.FileStatus.pathSuffix[1] == testFolder, TRUE)
  # MKDIRS - check 2 - GETFILESTATUS
  res <- adls.file.info(asc, azureDataLakeAccount, "", verbose = verbose)
  expect_is(res, "data.frame")
  expect_gte(nrow(res), 1)
  expect_equal(ncol(res), 11)
  # pathsuffix of a directory in getfilestatus will be empty!
  expect_equal(res$FileStatus.pathSuffix == "", TRUE)

  # CREATE
  res <- adls.create(asc, azureDataLakeAccount, paste0(testFolder, "/", testFile1), "755", FALSE, 4194304L, 3L, 268435456L, charToRaw("abcd"), verbose = verbose)
  expect_null(res)
  res <- adls.create(asc, azureDataLakeAccount, paste0(testFolder, "/", testFile2), "755", FALSE, 4194304L, 3L, 268435456L, charToRaw("efgh"), verbose = verbose)
  expect_null(res)
  # CREATE - check
  res <- adls.ls(asc, azureDataLakeAccount, testFolder, verbose = verbose)
  expect_is(res, "data.frame")
  expect_equal(nrow(res), 2)
  expect_equal(ncol(res), 12)
  expect_equal(res$FileStatuses.FileStatus.pathSuffix, c(testFile1, testFile2))
  expect_equal(res$FileStatuses.FileStatus.length, c(4, 4))
  # CREATE - check 2
  res <- adls.file.info(asc, azureDataLakeAccount, paste0(testFolder, "/", testFile1), verbose = verbose)
  expect_is(res, "data.frame")
  expect_equal(nrow(res), 1)
  expect_equal(ncol(res), 12)
  # pathsuffix of a file in getfilestatus will be empty!
  expect_equal(res$FileStatus.pathSuffix == "", TRUE)
  expect_equal(res$FileStatus.length, 4)
  # CREATE - check 3 - LS on a file
  res <- adls.ls(asc, azureDataLakeAccount, paste0(testFolder, "/", testFile1), verbose = verbose)
  expect_is(res, "data.frame")
  expect_equal(nrow(res), 1)
  expect_equal(ncol(res), 12)
  # pathsuffix of a file in liststatus will be empty!
  expect_equal(res$FileStatuses.FileStatus.pathSuffix == "", TRUE)
  expect_equal(res$FileStatuses.FileStatus.length, c(4))

  # READ (OPEN) # CREATE - check 3
  res <- adls.read.direct(asc, azureDataLakeAccount, paste0(testFolder, "/", testFile1), length = 2L, bufferSize = 4194304L, verbose = verbose)
  expect_equal(rawToChar(res), "ab")
  res <- adls.read.direct(asc, azureDataLakeAccount, paste0(testFolder, "/", testFile2), 2L, 2L, 4194304L, verbose = verbose)
  expect_equal(rawToChar(res), "gh")

  # APPEND
  res <- adls.append.direct(asc, azureDataLakeAccount, paste0(testFolder, "/", testFile1), 4194304L, charToRaw("stuv"), verbose = verbose)
  expect_null(res)
  res <- adls.append.direct(asc, azureDataLakeAccount, paste0(testFolder, "/", testFile2), 4194304L, charToRaw("wxyz"), verbose = verbose)
  expect_null(res)
  # APPEND - check
  res <- adls.ls(asc, azureDataLakeAccount, testFolder, verbose = verbose)
  expect_is(res, "data.frame")
  expect_equal(nrow(res), 2)
  expect_equal(ncol(res), 12)
  expect_equal(res$FileStatuses.FileStatus.pathSuffix, c(testFile1, testFile2))
  expect_equal(res$FileStatuses.FileStatus.length, c(8, 8))
  # APPEND - check 2 - READ (OPEN)
  res <- adls.read.direct(asc, azureDataLakeAccount, paste0(testFolder, "/", testFile1), verbose = verbose)
  expect_equal(rawToChar(res), "abcdstuv")
  res <- adls.read.direct(asc, azureDataLakeAccount, paste0(testFolder, "/", testFile2), verbose = verbose)
  expect_equal(rawToChar(res), "efghwxyz")

  # RENAME 1 (file, overwrite = false)
  res <- adls.rename(asc, azureDataLakeAccount, 
                      paste0(testFolder, "/", testFile1), # src
                      paste0(testFolder, "/", testFile2), # dest
                      verbose = verbose)
  expect_false(res)

  # RENAME 2 (file, overwrite = true)
  res <- adls.rename(asc, azureDataLakeAccount, 
                             paste0(testFolder, "/", testFile1), # src
                             paste0(testFolder, "/", testFile2), # dest
                             overwrite = TRUE,
                             verbose = verbose)
  expect_true(res)
  # RENAME 2 - check
  res <- adls.file.info(asc, azureDataLakeAccount, paste0(testFolder, "/", testFile2), verbose = verbose)
  expect_is(res, "data.frame")
  expect_equal(nrow(res), 1)
  expect_equal(ncol(res), 12)
  # pathsuffix of a file in getfilestatus will be empty!
  expect_equal(res$FileStatus.pathSuffix == "", TRUE)
  expect_equal(res$FileStatus.length, 8)

  # RENAME 3 (folder)
  res <- adls.rename(asc, azureDataLakeAccount, 
                             testFolder,       # src
                             testFolderRename, # dest
                             verbose = verbose)
  expect_true(res)
  # RENAME 3 - check
  res <- adls.file.info(asc, azureDataLakeAccount, testFolderRename, verbose = verbose)
  expect_is(res, "data.frame")
  expect_gte(nrow(res), 1)
  expect_equal(ncol(res), 11)

  # RENAME 4 (folder rename back to same name)
  res <- adls.rename(asc, azureDataLakeAccount, 
                             testFolderRename, # src
                             testFolder,       # dest
                             verbose = verbose)
  expect_true(res)
  # RENAME 4 - check
  res <- adls.file.info(asc, azureDataLakeAccount, testFolder, verbose = verbose)
  expect_is(res, "data.frame")
  expect_gte(nrow(res), 1)
  expect_equal(ncol(res), 11)

  # MSCONCAT
  res <- adls.create(asc, azureDataLakeAccount, testFileConcatDest,
                             "755", FALSE, 4194304L, 3L, 268435456L, charToRaw("1234"), verbose = verbose)
  expect_null(res)
  res <- adls.create(asc, azureDataLakeAccount, testFileConcatSrc1,
                             "755", FALSE, 4194304L, 3L, 268435456L, charToRaw("abcd"), verbose = verbose)
  expect_null(res)
  res <- adls.create(asc, azureDataLakeAccount, testFileConcatSrc2,
                             "755", FALSE, 4194304L, 3L, 268435456L, charToRaw("efgh"), verbose = verbose)
  expect_null(res)
  res <- adls.create(asc, azureDataLakeAccount, testFileConcatSrc3,
                             "755", FALSE, 4194304L, 3L, 268435456L, charToRaw("ijkl"), verbose = verbose)
  expect_null(res)
  res <- adls.concat(asc, azureDataLakeAccount, testFileConcatDest,
                             c(testFileConcatSrc1, testFileConcatSrc2, testFileConcatSrc3),
                             verbose = verbose)
  expect_null(res)
  # MSCONCAT - check - 1 - GFS
  res <- adls.file.info(asc, azureDataLakeAccount, testFileConcatDest, verbose = verbose)
  expect_is(res, "data.frame")
  expect_equal(nrow(res), 1)
  expect_equal(ncol(res), 12)
  # pathsuffix of a file in getfilestatus will be empty!
  expect_equal(res$FileStatus.pathSuffix == "", TRUE)
  expect_equal(res$FileStatus.length, 16)
  # MSCONCAT - check - 2 - contents of concatenated file
  res <- adls.read.direct(asc, azureDataLakeAccount, testFileConcatDest, verbose = verbose)
  expect_equal(rawToChar(res), "1234abcdefghijkl")
  # MSCONCAT - check - 3 - src files are deleted
  expect_error(adls.file.info(asc, azureDataLakeAccount, testFileConcatSrc1, verbose = verbose))
  expect_error(adls.file.info(asc, azureDataLakeAccount, testFileConcatSrc2, verbose = verbose))
  expect_error(adls.file.info(asc, azureDataLakeAccount, testFileConcatSrc3, verbose = verbose))

  # DELETE
  res <- adls.delete(asc, azureDataLakeAccount, testFolder, TRUE, verbose = verbose)
  expect_true(res)
})

# test_that("Encoding special characters in R SDK for azure data lake account", { ----

test_that("Encoding special characters in R SDK for azure data lake account", {
  skip_if_missing_config(settingsfile)

  printADLSMessage("test-7-datalake.R", "test_that",
                   "Encoding special characters in R SDK for azure data lake account",
                   NULL)

  verbose <- FALSE

  testFolder <- "tempfolder1SplChars" # also test for special characters and utf16 languages
  Encoding(testFolder) <- "UTF-8" # need to explicitly set the string encoding in case of other language characters!?

  # cleanup the account before starting tests!
  try(
    adls.delete(asc, azureDataLakeAccount, testFolder, TRUE, verbose = verbose)
  )

  # Generate test data
  fileName <- ""
  Encoding(fileName) <- "UTF-8"
  char <- ""
  Encoding(char) <- "UTF-8"
  uploadContentData <- c()
  nums <- seq(32:126)
  for (num in nums) {
    chr <- rawToChar(as.raw(num))
    Encoding(chr) <- "UTF-8"
    if(chr == '?') {
      uploadContentData[1] <- paste0(
        "fileCharSupported-"
        , format(Sys.time(), "%d-%m-%Y %H-%M-%OS")
        , "a_"
        , "?"
        , "_FILE"
      )
    } else if(chr == '#') {
      uploadContentData[2] <- paste0(
        "fileCharSupported-"
        , format(Sys.time(), "%d-%m-%Y %H-%M-%OS")
        , "a_"
        , "#"
        , "_FILE"
      )
    } else {
      fileName <- paste0(fileName, chr)
      Encoding(fileName) <- "UTF-8"
    }
  }
  # Reason to split into 2 chunks than 1 single chunk was to overcome the limit url size limit.
  uploadContentData[3] <- paste0(
    "fileCharSupported-"
    , format(Sys.time(), "%d-%m-%Y %H-%M-%OS")
    , "a_"
    , substr(fileName, 1, (getContentSize(fileName)/2))
    , "_FILE"
  )
  uploadContentData[4] <- paste0(
    "fileCharSupported-"
    , format(Sys.time(), "%d-%m-%Y %H-%M-%OS")
    , "a_"
    , substr(fileName, (getContentSize(fileName)/2)+1, getContentSize(fileName)) 
    , "_FILE"
  )

  # reassign directly
  uploadContentData[3] <- paste0(
    "fileCharSupported-"
    , format(Sys.time(), "%d-%m-%Y %H-%M-%OS")
    , "a_"
    , " !\"$%25&'()*+,-.0123456789;<=>@ABCDEFGHIJKLMNOP"
    , "_FILE"
  )
  uploadContentData[4] <- paste0(
    "fileCharSupported-"
    , format(Sys.time(), "%d-%m-%Y %H-%M-%OS")
    , "a_"
    , "QRSTUVWXYZ[]^_`abcdefghijklmnopqrstuvwxyz{|}~"
    , "_FILE"
  )

  # Start Tests
  res <- adls.mkdir(asc, azureDataLakeAccount, testFolder, verbose = verbose)
  expect_true(res)
  for (testFolderEnc in uploadContentData) {
    Encoding(testFolderEnc) <- "UTF-8"
    if(verbose) printADLSMessage("test-7-datalake.R"
                                 , "test_that(\"Encoding special characters in R SDK for azure data lake account\""
                                 , paste0("testFolderEnc=", testFolderEnc), NULL)
    res <- adls.mkdir(asc, azureDataLakeAccount, paste0(testFolder, "/", testFolderEnc), verbose = TRUE)
    expect_true(res)
  }

  # DELETE
  res <- adls.delete(asc, azureDataLakeAccount, testFolder, TRUE, verbose = verbose)
  expect_true(res)
})

# test_that("Can append and read using buffered IO streams from files in R SDK for azure data lake account", { ----

datafile2MB <- paste0(getwd(), "/data/test2MB.bin")
datafile4MB <- paste0(getwd(), "/data/test4MB.bin")
datafile6MB <- paste0(getwd(), "/data/test6MB.bin")

test_that("Can append and read using buffered IO streams from files in R SDK for azure data lake account", {
  skip_if_missing_config(settingsfile)

  printADLSMessage("test-7-datalake.R", "test_that",
                   "Can append and read using buffered IO streams from files in R SDK for azure data lake account",
                   NULL)

  verbose <- FALSE

  # cleanup the account before starting tests!
  try(
    adls.delete(asc, azureDataLakeAccount, "tempfolder1", TRUE, verbose = verbose)
  )

  # MKDIRS
  res <- adls.mkdir(asc, azureDataLakeAccount, "tempfolder1", verbose = verbose)
  expect_true(res)

  # CREATE
  res <- adls.create(asc, azureDataLakeAccount, "tempfolder1/test2MB.bin", "755", verbose = verbose)
  expect_null(res)
  # APPEND - test2MB.bin
  binData <- readBin(con = datafile2MB, what = "raw", n = 2097152)
  adlFOS <- adls.append(asc, azureDataLakeAccount, "tempfolder1/test2MB.bin", verbose = verbose)
  expect_is(adlFOS, "adlFileOutputStream")
  res <- adls.fileoutputstream.write(adlFOS, binData, 1, 2097152L, verbose = verbose)
  expect_null(res)
  res <- adls.fileoutputstream.close(adlFOS)
  expect_null(res)
  # APPEND - test2MB.bin - check
  res <- adls.file.info(asc, azureDataLakeAccount, "tempfolder1/test2MB.bin", verbose = verbose)
  expect_is(res, "data.frame")
  expect_equal(nrow(res), 1)
  expect_equal(ncol(res), 12)
  # pathsuffix of a file in getfilestatus will be empty!
  expect_equal(res$FileStatus.pathSuffix == "", TRUE)
  expect_equal(res$FileStatus.length, 2097152)
  # OPEN(READ) - test2MB.bin
  adlFIS <- adls.read(asc, azureDataLakeAccount, "tempfolder1/test2MB.bin", verbose = verbose)
  buffer <- raw(2097152)
  res <- adls.fileinputstream.readfully(adlFIS, 0L, buffer, 1L, 2097152L, verbose = verbose)
  expect_equal(res[[1]], 2097152)
  expect_equal(res[[2]], binData)
  res <- adls.fileinputstream.close(adlFIS, verbose = verbose)
  # OPEN(READ_BUFFERED) - test2MB.bin
  adlFIS <- adls.read(asc, azureDataLakeAccount, "tempfolder1/test2MB.bin")
  buffer <- raw(2097152)
  res <- adls.fileinputstream.read(adlFIS, buffer, 1L, 2097152L, verbose = verbose)
  expect_equal(res[[1]], 2097152)
  expect_equal(res[[2]], binData)
  res <- adls.fileinputstream.close(adlFIS, verbose = verbose)
  # OPEN(SEEK, READ_BUFFERED) - test2MB.bin
  adlFIS <- adls.read(asc, azureDataLakeAccount, "tempfolder1/test2MB.bin", verbose = verbose)
  res <- adls.fileinputstream.seek(adlFIS, 1048576)
  expect_null(res)
  res <- adls.fileinputstream.getpos(adlFIS)
  expect_equal(res, 1048576)
  buffer <- raw(1048576)
  res <- adls.fileinputstream.read(adlFIS, buffer, 1L, 1048576L, verbose = verbose)
  expect_equal(res[[1]], 1048576)
  expect_equal(res[[2]], binData[1048577:2097152])
  res <- adls.fileinputstream.close(adlFIS, verbose = verbose)
  # OPEN(SKIP, READ_BUFFERED) - test2MB.bin
  adlFIS <- adls.read(asc, azureDataLakeAccount, "tempfolder1/test2MB.bin", verbose = verbose)
  res <- adls.fileinputstream.skip(adlFIS, 1048576)
  expect_equal(res, 1048576)
  adls.fileinputstream.getpos(adlFIS)
  buffer <- raw(1048576)
  res <- adls.fileinputstream.read(adlFIS, buffer, 1L, 1048576L, verbose = verbose)
  expect_equal(res[[1]], 1048576)
  expect_equal(res[[2]], binData[1048577:2097152])
  res <- adls.fileinputstream.close(adlFIS, verbose = verbose)

  # CREATE
  res <- adls.create(asc, azureDataLakeAccount, "tempfolder1/test4MB.bin", "755", verbose = verbose)
  expect_null(res)
  # APPEND - test4MB.bin
  binData <- readBin(con = datafile4MB, what = "raw", n = 4194304)
  adlFOS <- adls.append(asc, azureDataLakeAccount, "tempfolder1/test4MB.bin", verbose = verbose)
  expect_is(adlFOS, "adlFileOutputStream")
  res <- adls.fileoutputstream.write(adlFOS, binData, 1, 4194304L, verbose = verbose)
  expect_null(res)
  res <- adls.fileoutputstream.close(adlFOS, verbose = verbose)
  expect_null(res)
  # APPEND - test4MB.bin - check
  res <- adls.file.info(asc, azureDataLakeAccount, "tempfolder1/test4MB.bin", verbose = verbose)
  expect_is(res, "data.frame")
  expect_equal(nrow(res), 1)
  expect_equal(ncol(res), 12)
  # pathsuffix of a file in getfilestatus will be empty!
  expect_equal(res$FileStatus.pathSuffix == "", TRUE)
  expect_equal(res$FileStatus.length, 4194304)
  # OPEN(READ) - test4MB.bin
  adlFIS <- adls.read(asc, azureDataLakeAccount, "tempfolder1/test4MB.bin", verbose = verbose)
  buffer <- raw(4194304)
  res <- adls.fileinputstream.readfully(adlFIS, 0L, buffer, 1L, 4194304L, verbose = verbose)
  expect_equal(res[[1]], 4194304)
  expect_equal(res[[2]], binData)
  res <- adls.fileinputstream.close(adlFIS, verbose = verbose)
  # OPEN(READ_BUFFERED) - test4MB.bin
  adlFIS <- adls.read(asc, azureDataLakeAccount, "tempfolder1/test4MB.bin", verbose = verbose)
  buffer <- raw(4194304)
  res <- adls.fileinputstream.read(adlFIS, buffer, 1L, 4194304L, verbose = verbose)
  expect_equal(res[[1]], 4194304)
  expect_equal(res[[2]], binData)
  res <- adls.fileinputstream.close(adlFIS, verbose = verbose)

  # CREATE
  res <- adls.create(asc, azureDataLakeAccount, "tempfolder1/test6MB.bin", "755", verbose = verbose)
  expect_null(res)
  # APPEND - test6MB.bin
  binData <- readBin(con = datafile6MB, what = "raw", n = 6291456)
  adlFOS <- adls.append(asc, azureDataLakeAccount, "tempfolder1/test6MB.bin", verbose = verbose)
  expect_is(adlFOS, "adlFileOutputStream")
  res <- adls.fileoutputstream.write(adlFOS, binData, 1, 6291456L, verbose = verbose)
  expect_null(res)
  res <- adls.fileoutputstream.close(adlFOS, verbose = verbose)
  expect_null(res)
  # APPEND - test6MB.bin - check
  res <- adls.file.info(asc, azureDataLakeAccount, "tempfolder1/test6MB.bin", verbose = verbose)
  expect_is(res, "data.frame")
  expect_equal(nrow(res), 1)
  expect_equal(ncol(res), 12)
  # pathsuffix of a file in getfilestatus will be empty!
  expect_equal(res$FileStatus.pathSuffix == "", TRUE)
  expect_equal(res$FileStatus.length, 6291456)
  # OPEN(READ) - test6MB.bin
  adlFIS <- adls.read(asc, azureDataLakeAccount, "tempfolder1/test6MB.bin", verbose = verbose)
  buffer <- raw(6291456)
  res <- adls.fileinputstream.readfully(adlFIS, 0L, buffer, 1L, 6291456L, verbose = verbose)
  expect_equal(res[[1]], 6291456)
  expect_equal(res[[2]], binData)
  res <- adls.fileinputstream.close(adlFIS, verbose = verbose)
  # OPEN(READ_BUFFERED) - test6MB.bin
  adlFIS <- adls.read(asc, azureDataLakeAccount, "tempfolder1/test6MB.bin", verbose = verbose)
  buffer <- raw(6291456)
  res <- adls.fileinputstream.read(adlFIS, buffer, 1L, 6291456L, verbose = verbose)
  expect_equal(res[[1]], 4194304)
  expect_equal(res[[2]][1:4194304], binData[1:4194304])
  res <- adls.fileinputstream.close(adlFIS, verbose = verbose)

  # CREATE
  res <- adls.create(asc, azureDataLakeAccount, "tempfolder1/test6MBWA.bin", "755", verbose = verbose)
  expect_null(res)
  # **** APPEND - 2MB to test6MBWA.bin
  binData <- readBin(con = datafile2MB, what = "raw", n = 2097152)
  binDataWA <- binData
  adlFOS <- adls.append(asc, azureDataLakeAccount, "tempfolder1/test6MBWA.bin", verbose = verbose)
  expect_is(adlFOS, "adlFileOutputStream")
  res <- adls.fileoutputstream.write(adlFOS, binData, 1, 2097152L, verbose = verbose)
  expect_null(res)
  res <- adls.fileoutputstream.close(adlFOS, verbose = verbose)
  expect_null(res)
  # CHECK - APPEND - 2MB to test6MBWA.bin
  res <- adls.file.info(asc, azureDataLakeAccount, "tempfolder1/test6MBWA.bin", verbose = verbose)
  expect_is(res, "data.frame")
  expect_equal(nrow(res), 1)
  expect_equal(ncol(res), 12)
  # pathsuffix of a file in getfilestatus will be empty!
  expect_equal(res$FileStatus.pathSuffix == "", TRUE)
  expect_equal(res$FileStatus.length, 2097152)
  # **** APPEND - 4MB to test6MBWA.bin
  binData <- readBin(con = datafile4MB, what = "raw", n = 4194304)
  binDataWA[2097153:6291456] <- binData[1:4194304]
  adlFOS <- adls.append(asc, azureDataLakeAccount, "tempfolder1/test6MBWA.bin", verbose = verbose)
  expect_is(adlFOS, "adlFileOutputStream")
  res <- adls.fileoutputstream.write(adlFOS, binData, 1, 4194304L, verbose = verbose)
  expect_null(res)
  res <- adls.fileoutputstream.close(adlFOS, verbose = verbose)
  expect_null(res)
  # CHECK - APPEND - 4MB to test6MBWA.bin
  res <- adls.file.info(asc, azureDataLakeAccount, "tempfolder1/test6MBWA.bin", verbose = verbose)
  expect_is(res, "data.frame")
  expect_equal(nrow(res), 1)
  expect_equal(ncol(res), 12)
  # pathsuffix of a file in getfilestatus will be empty!
  expect_equal(res$FileStatus.pathSuffix == "", TRUE)
  expect_equal(res$FileStatus.length, 6291456)
  # OPEN(READ) - test6MBWA.bin
  adlFIS <- adls.read(asc, azureDataLakeAccount, "tempfolder1/test6MBWA.bin", verbose = verbose)
  buffer <- raw(6291456)
  res <- adls.fileinputstream.readfully(adlFIS, 0L, buffer, 1L, 6291456L, verbose = verbose)
  expect_equal(res[[1]], 6291456)
  expect_equal(res[[2]], binDataWA)
  res <- adls.fileinputstream.close(adlFIS, verbose = verbose)
  # OPEN(READ) - test6MBWA.bin- check with offset
  adlFIS <- adls.read(asc, azureDataLakeAccount, "tempfolder1/test6MBWA.bin", verbose = verbose)
  buffer <- raw(6291456)
  res <- adls.fileinputstream.readfully(adlFIS, 2097152L, buffer, 2097153L, 4194304L, verbose = verbose)
  expect_equal(res[[1]], 4194304)
  expect_equal(res[[2]][2097153:6291456], binData)
  res <- adls.fileinputstream.close(adlFIS, verbose = verbose)
  # OPEN(READ_BUFFERED) - test6MBWA.bin
  adlFIS <- adls.read(asc, azureDataLakeAccount, "tempfolder1/test6MBWA.bin", verbose = verbose)
  buffer <- raw(6291456)
  res <- adls.fileinputstream.read(adlFIS, buffer, 1L, 6291456L, verbose = verbose)
  expect_equal(res[[1]], 4194304)
  expect_equal(res[[2]][1:4194304], binDataWA[1:4194304])
  res <- adls.fileinputstream.close(adlFIS, verbose = verbose)

  # DELETE
  res <- adls.delete(asc, azureDataLakeAccount, "tempfolder1", TRUE, verbose = verbose)
  expect_true(res)
})

# test_that("Different retry policies for various APIs in R SDK for azure data lake account", { ----

test_that("Different retry policies for various APIs in R SDK for azure data lake account", {
  skip_if_missing_config(settingsfile)

  printADLSMessage("test-7-datalake.R", "test_that",
                   "Different retry policies for various APIs in R SDK for azure data lake account",
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
  #stub(adls.mkdir, 'callAzureDataLakeRestEndPoint', mockCallAzureDataLakeRestEndPoint, depth = 2)
  with_mock(
    # use fully qualified name of function to be mocked, else this doesnt work
    "AzureSMR:::callAzureDataLakeRestEndPoint" = mockCallAzureDataLakeRestEndPoint,
    res <- adls.mkdir(asc, azureDataLakeAccount, testFolder, verbose = verbose),
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
                   charToRaw("{\"RemoteException\":{\"exception\":\"RuntimeException\"
                             ,\"message\":\"MkDir failed with error xxx\"
                             ,\"javaClassName\":\"java.lang.RuntimeException\"}}")),
    createMockResponse(200,  # retry - 5 - retry limit exceeded - mock success response - retries finished, so overall FAIL
                   charToRaw("{\"boolean\":true}")), 
    cycle = FALSE)
  with_mock(
    "AzureSMR:::callAzureDataLakeRestEndPoint" = mockCallAzureDataLakeRestEndPoint,
    expect_error(adls.mkdir(asc, azureDataLakeAccount, testFolder, verbose = verbose)),
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
    expect_error(adls.file.info(asc, azureDataLakeAccount, testFolder, verbose = verbose)),
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
    res <- adls.create(asc, azureDataLakeAccount, testFolder, overwrite = TRUE, verbose = verbose),
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
    expect_error(adls.create(asc, azureDataLakeAccount, testFolder, overwrite = FALSE, verbose = verbose)),
    expect_called(mockCallAzureDataLakeRestEndPoint, 3)
  )
})

# test_that("Bad offset error handling for appends in R SDK for azure data lake account", { ----

test_that("Bad offset error handling for appends in R SDK for azure data lake account", {
  skip_if_missing_config(settingsfile)

  printADLSMessage("test-7-datalake.R", "test_that",
                   "Bad offset error handling for appends in R SDK azure data lake account",
                   NULL)

  verbose <- FALSE
  testFolder <- "tempfolder3BadOffset"
  testFile <- "test2n2MB.bin"

  # cleanup the account before starting tests!
  try(
    adls.delete(asc, azureDataLakeAccount, testFolder, TRUE, verbose = verbose)
  )

  # MKDIRS
  res <- adls.mkdir(asc, azureDataLakeAccount, testFolder, verbose = verbose)
  expect_true(res)

  # CREATE
  res <- adls.create(asc, azureDataLakeAccount, paste0(testFolder, "/", testFile), "755", verbose = verbose)
  expect_null(res)

  # APPEND - test2MB.bin
  binData <- readBin(con = datafile2MB, what = "raw", n = 2097152)
  adlFOS <- adls.append(asc, azureDataLakeAccount, paste0(testFolder, "/", testFile), verbose = verbose)
  expect_is(adlFOS, "adlFileOutputStream")
  # Write to the stream
  res <- adls.fileoutputstream.write(adlFOS, binData, 1, 2097152L, verbose = verbose)
  expect_null(res)

  # MKDIR - mock with 4 fail and 5th success response.
  # This should PASS since the last(2nd) retry succeeded.
  mockCallAzureDataLakeRestEndPoint <- mock(
    createMockResponse(408), # initial call - mock error response - request times out during an append
    createMockResponse(400, # retry 1 - mock error response - retry results in a Bad Offset => previous 
                            # request succeeded on backend
                       charToRaw(paste0("{\"RemoteException\":"
                                        ,"{"
                                        ,"\"exception\":\"BadOffsetException\""
                                        ,",\"message\":\"APPEND failed with error 0x83090015 (Bad request. Invalid offset.). "
                                        ,"[71f3c65c-b077-49ce-8f43-e1f2eea2f2fa][2018-06-26T23:35:12.1960963-07:00]\""
                                        ,",\"javaClassName\":\"org.apache.hadoop.fs.adl.BadOffsetException\""
                                        ,"}"
                                        ,"}"
                                        )
                                 )
                       ),
    createMockResponse(500), # retry - 1 - mock error response to trigger a retry within badoffset error handling
    createMockResponse(200), # retry - 2 - last retry - mock success response - overall PASS
    cycle = FALSE)
  with_mock(
    # use fully qualified name of function to be mocked, else this doesnt work
    "AzureSMR:::callAzureDataLakeRestEndPoint" = mockCallAzureDataLakeRestEndPoint,
    res <- adls.fileoutputstream.close(adlFOS), # close flushes the buffered data as 4MB chunks
    expect_called(mockCallAzureDataLakeRestEndPoint, 4)
  )
  expect_null(res)

  # DELETE
  res <- adls.delete(asc, azureDataLakeAccount, testFolder, TRUE, verbose = verbose)
  expect_true(res)
})
