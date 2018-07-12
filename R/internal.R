
azureApiHeaders <- function(token) {
  headers <- c(Host = "management.azure.com",
               Authorization = token,
                `Content-type` = "application/json")
  httr::add_headers(.headers = headers)
}

# convert verbose=TRUE to httr verbose
set_verbosity <- function(verbose = FALSE) {
  if (verbose) httr::verbose(TRUE) else NULL
}

extractUrlArguments <- function(x) {
  ptn <- ".*\\?(.*?)"
  args <- grepl("\\?", x)
  z <- if (args) gsub(ptn, "\\1", x) else ""
  if (z == "") {
    ""
  } else {
    z <- strsplit(z, "&")[[1]]
    z <- sort(z)
    z <- paste(z, collapse = "\n")
    z <- gsub("=", ":", z)
    paste0("\n", z)
  }
}

callAzureStorageApi <- function(url, verb = "GET", storageKey, storageAccount,
                   headers = NULL, container = NULL, CMD, size = getContentSize(content), contenttype = NULL,
                   content = NULL,
                   verbose = FALSE) {
  dateStamp <- httr::http_date(Sys.time())

  verbosity <- set_verbosity(verbose) 

  if (missing(CMD) || is.null(CMD)) CMD <- extractUrlArguments(url)

    sig <- createAzureStorageSignature(url = url, verb = verb,
      key = storageKey, storageAccount = storageAccount, container = container,
      headers = headers, CMD = CMD, size = size,
      contenttype = contenttype, dateStamp = dateStamp, verbose = verbose)

  azToken <- paste0("SharedKey ", storageAccount, ":", sig)

  switch(verb, 
  "GET" = GET(url, add_headers(.headers = c(Authorization = azToken,
                                    `Content-Length` = "0",
                                    `x-ms-version` = "2017-04-17",
                                    `x-ms-date` = dateStamp)
                                    ),
    verbosity),
  "PUT" = PUT(url, add_headers(.headers = c(Authorization = azToken,
                                         `Content-Length` = size,
                                         `x-ms-version` = "2017-04-17",
                                         `x-ms-date` = dateStamp,
                                         `x-ms-blob-type` = "Blockblob",
                                         `Content-type` = contenttype)),
           body = content,
    verbosity)
  )
}

getContentSize<- function(obj) {
    switch(class(obj),
         "raw" = length(obj),
         "character" = nchar(obj),
         nchar(obj))
}

createAzureStorageSignature <- function(url, verb, 
  key, storageAccount, container = NULL,
  headers = NULL, CMD = NULL, size = NULL, contenttype = NULL, dateStamp, verbose = FALSE) {

  if (missing(dateStamp)) {
    dateStamp <- httr::http_date(Sys.time())
  }

  arg1 <- if (length(headers)) {
    paste0(headers, "\nx-ms-date:", dateStamp, "\nx-ms-version:2017-04-17")
  } else {
    paste0("x-ms-date:", dateStamp, "\nx-ms-version:2017-04-17")
  }

  arg2 <- paste0("/", storageAccount, "/", container, CMD)

  SIG <- paste0(verb, "\n\n\n", size, "\n\n", contenttype, "\n\n\n\n\n\n\n",
                   arg1, "\n", arg2)
  if (verbose) message(paste0("TRACE: STRINGTOSIGN: ", SIG))
  base64encode(hmac(key = base64decode(key),
                    object = iconv(SIG, "ASCII", to = "UTF-8"),
                    algo = "sha256",
                    raw = TRUE)
                   )
}

x_ms_date <- function() httr::http_date(Sys.time())

azure_storage_header <- function(shared_key, date = x_ms_date(), content_length = 0) {
  if(!is.character(shared_key)) stop("Expecting a character for `shared_key`")
  headers <- c(
      Authorization = shared_key,
      `Content-Length` = as.character(content_length),
      `x-ms-version` = "2017-04-17",
      `x-ms-date` = date
  )
  add_headers(.headers = headers)
}

getSig <- function(azureActiveContext, url, verb, key, storageAccount,
                   headers = NULL, container = NULL, CMD = NULL, size = NULL, contenttype = NULL,
                   date = x_ms_date(), verbose = FALSE) {

  arg1 <- if (length(headers)) {
    paste0(headers, "\nx-ms-date:", date, "\nx-ms-version:2017-04-17")
  } else {
    paste0("x-ms-date:", date, "\nx-ms-version:2017-04-17")
  }

  arg2 <- paste0("/", storageAccount, "/", container, CMD)

  SIG <- paste0(verb, "\n\n\n", size, "\n\n", contenttype, "\n\n\n\n\n\n\n",
                   arg1, "\n", arg2)
  if (verbose) message(paste0("TRACE: STRINGTOSIGN: ", SIG))
  base64encode(hmac(key = base64decode(key),
                    object = iconv(SIG, "ASCII", to = "UTF-8"),
                    algo = "sha256",
                    raw = TRUE)
                   )
  }

getAzureErrorMessage <- function(r) {
  msg <- paste0(as.character(sys.call(1))[1], "()") # Name of calling fucntion
  addToMsg <- function(x) {
    if (!is.null(x)) x <- strwrap(x)
    if(is.null(x)) msg else c(msg, x)
  }
  if(inherits(content(r), "xml_document")){
    rr <- XML::xmlToList(XML::xmlParse(content(r)))
    msg <- addToMsg(rr$Code)
    msg <- addToMsg(rr$Message)
    msg <- addToMsg(rr$AuthenticationErrorDetail)
  } else {
    rr <- content(r)
    msg <- addToMsg(rr$code)
    msg <- addToMsg(rr$message)
    msg <- addToMsg(rr$error$message)
    
    msg <- addToMsg(rr$Code)
    msg <- addToMsg(rr$Message)
    msg <- addToMsg(rr$Error$Message)
    
  }
  msg <- addToMsg(paste0("Return code: ", status_code(r)))
  msg <- paste(msg, collapse = "\n")
  return(msg)
}

stopWithAzureError <- function(r) {
  if (status_code(r) < 300) return()
  msg <- getAzureErrorMessage(r)
  stop(msg, call. = FALSE)
}

extractResourceGroupname <- function(x) gsub(".*?/resourceGroups/(.*?)(/.*)*$",  "\\1", x)
extractSubscriptionID    <- function(x) gsub(".*?/subscriptions/(.*?)(/.*)*$",   "\\1", x)
extractStorageAccount    <- function(x) gsub(".*?/storageAccounts/(.*?)(/.*)*$", "\\1", x)


refreshStorageKey <- function(azureActiveContext, storageAccount, resourceGroup){
  if (storageAccount != azureActiveContext$storageAccount ||
      length(azureActiveContext$storageKey) == 0
  ) {
    message("Fetching Storage Key..")
    azureSAGetKey(azureActiveContext, resourceGroup = resourceGroup, storageAccount = storageAccount)
  } else {
    azureActiveContext$storageKey
  }
}


updateAzureActiveContext <- function(x, storageAccount, storageKey, resourceGroup, container, blob, directory) {
  # updates the active azure context in place
  if (!is.null(x)) {
    assert_that(is.azureActiveContext(x))
    if (!missing(storageAccount)) x$storageAccount <- storageAccount
    if (!missing(resourceGroup))  x$resourceGroup  <- resourceGroup
    if (!missing(storageKey))     x$storageKey     <- storageKey
    if (!missing(container)) x$container <- container
    if (!missing(blob)) x$blob <- blob
    if (!missing(directory)) x$directory <- directory
  }
  TRUE
}

## https://gist.github.com/cbare/5979354
## Version 4 UUIDs have the form xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx
## where x is any hexadecimal digit and y is one of 8, 9, A, or B
## e.g., f47ac10b-58cc-4372-a567-0e02b2c3d479
uuid <- function(uppercase=FALSE) {
  
  hex_digits <- c(as.character(0:9), letters[1:6])
  hex_digits <- if (uppercase) toupper(hex_digits) else hex_digits
  
  y_digits <- hex_digits[9:12]
  
  paste(
    paste0(sample(hex_digits, 8, replace=TRUE), collapse=''),
    paste0(sample(hex_digits, 4, replace=TRUE), collapse=''),
    paste0('4', paste0(sample(hex_digits, 3, replace=TRUE), collapse=''), collapse=''),
    paste0(sample(y_digits,1), paste0(sample(hex_digits, 3, replace=TRUE), collapse=''), collapse=''),
    paste0(sample(hex_digits, 12, replace=TRUE), collapse=''),
    sep='-')
}

## https://stackoverflow.com/questions/40059573/r-get-current-time-in-milliseconds
## R function to get current time in nanoseconds
getCurrentTimeInNanos <- function() {
  return(as.numeric(Sys.time())*10^9)
}

# ADLS Global variables ----

{
  # create a syncFlagEnum object used by the Azure Data Lake Store functions.
  syncFlagEnum <- list("DATA", "METADATA", "CLOSE", "PIPELINE")
  names(syncFlagEnum) <- syncFlagEnum
  # create a retryPolicyEnum object used by the Azure Data Lake Store functions.
  retryPolicyEnum <- list("EXPONENTIALBACKOFF", "NONIDEMPOTENT")
  names(retryPolicyEnum) <- retryPolicyEnum
}

# ADLS Helper Functions ----

getAzureDataLakeSDKVersion <- function() {
  return("1.3.0")
}

getAzureDataLakeSDKUserAgent <- function() {
  sysInf <- as.list(strsplit(Sys.info(), "\t"))
  adlsUA <- paste0("ADLSRSDK"
                   , "-", getAzureDataLakeSDKVersion()
                   , "/", sysInf$sysname, "-", sysInf$release
                   , "-", sysInf$version
                   , "-", sysInf$machine
                   , "/", R.version$version.string
  )
  return(adlsUA)
}

getAzureDataLakeBasePath <- function(azureDataLakeAccount) {
  basePath <- paste0("https://", azureDataLakeAccount, ".azuredatalakestore.net/webhdfs/v1/")
  return(basePath)
}

getAzureDataLakeApiVersion <- function() {
  return("&api-version=2018-02-01")
}

getAzureDataLakeApiVersionForConcat <- function() {
  return("&api-version=2018-05-01")
}

getAzureDataLakeDefaultBufferSize <- function() {
  return(as.integer(4 * 1024 * 1024))
}

getAzureDataLakeURLEncodedString <- function(strToEncode) {
  strEncoded <- URLencode(strToEncode, reserved = TRUE, repeated = TRUE)
  return(strEncoded)
}

# log printer for Azure Data Lake Store
printADLSMessage <- function(fileName, functionName, message, error = NULL) {
  msg <- paste0(Sys.time()
    , " [", fileName, "]"
    , " ", functionName
    , ": message=", message
    , ", error=", error 
  )
  print(msg)
}

# ADLS Ingress - AdlFileOutputStream ----

#' Create an adlFileOutputStream.
#' Create a container (`adlFileOutputStream`) for holding variables used by the Azure Data Lake Store data functions.
#'
#' @inheritParams setAzureContext
#' @param accountName the account name
#' @param relativePath Relative path of a file/directory
#' @param verbose Print tracing information (default FALSE).
#' @return An `adlFileOutputStream` object
#'
#' @family Azure Data Lake Store functions
adls.fileoutputstream.create <- function(azureActiveContext, accountName, relativePath, verbose = FALSE) {
  azEnv <- new.env(parent = emptyenv())
  azEnv <- as.adlFileOutputStream(azEnv)
  list2env(
    list(azureActiveContext = "", accountName = "", relativePath = ""),
    envir = azEnv
  )
  if (!missing(azureActiveContext)) azEnv$azureActiveContext <- azureActiveContext
  if (!missing(accountName)) azEnv$accountName <- accountName
  if (!missing(relativePath)) azEnv$relativePath <- relativePath
  azEnv$leaseId <- uuid()
  azEnv$blockSize <- getAzureDataLakeDefaultBufferSize()
  azEnv$buffer <- raw(0)
  # cursors/indices/offsets in R should start from 1 and NOT 0. 
  # Because of this there are many adjustments that need to be done throughout the code!
  azEnv$cursor <- 1L
  res <- adls.file.info(azureActiveContext, accountName, relativePath, verbose)
  azEnv$remoteCursor <- as.integer(res$FileStatus.length) # this remote cursor starts from 0
  azEnv$streamClosed <- FALSE
  azEnv$lastFlushUpdatedMetadata <- FALSE
  
  # additional param required to implement bad offset handling
  azEnv$numRetries <- 0
  
  return(azEnv)
}

adls.fileoutputstream.addtobuffer <- function(adlFileOutputStream, contents, off, len) {
  bufferlen <- getContentSize(adlFileOutputStream$buffer)
  cursor <- adlFileOutputStream$cursor
  if (len > bufferlen - (cursor - 1)) { # if requesting to copy more than remaining space in buffer
    stop("IllegalArgumentException: invalid buffer copy requested in adls.fileoutputstream.addtobuffer")
  }
  # optimized arraycopy
  adlFileOutputStream$buffer[cursor : (cursor + len - 1)] <- contents[off : (off + len - 1)]
  adlFileOutputStream$cursor <- as.integer(cursor + len)
}

adls.fileoutputstream.dozerolengthappend <- function(adlFileOutputStream, azureDataLakeAccount, relativePath, offset, verbose = FALSE) {
  resHttp <- adls.append.core(adlFileOutputStream$azureActiveContext, adlFileOutputStream,
                              azureDataLakeAccount, relativePath,
                              4194304L, contents = raw(0), contentSize = 0L,
                              leaseId = adlFileOutputStream$leaseId, sessionId = adlFileOutputStream$leaseId,
                              syncFlag = syncFlagEnum$METADATA, offsetToAppendTo = 0, verbose = verbose)
  stopWithAzureError(resHttp)
  # retrun a NULL (void)
  return(TRUE)
}

#' The Core Append API.
#'
#' @inheritParams setAzureContext
#' @param azureDataLakeAccount Name of the Azure Data Lake account.
#' @param adlFileOutputStream The adlFileOutputStream object to operate with.
#' @param relativePath Relative path of a file.
#' @param bufferSize Size of the buffer to be used.
#' @param contents raw contents to be written to the file.
#' @param contentSize size of `contents` to be written to the file.
#' @param leaseId a String containing the lease ID (generated by client). Can be null.
#' @param sessionId a String containing the session ID (generated by client). Can be null.
#' @param syncFlag
#'     Use `DATA` when writing more bytes to same file path. Most performant operation.
#'     Use `METADATA` when metadata for the
#'         file also needs to be updated especially file length
#'         retrieved from `adls.file.info` or `adls.ls` API call.
#'         Has an overhead of updating metadata operation.
#'     Use `CLOSE` when no more data is
#'         expected to be written in this path. Adl backend would
#'         update metadata, close the stream handle and
#'         release the lease on the
#'         path if valid leaseId is passed.
#'         Expensive operation and should be used only when last
#'         bytes are written.
#' @param offsetToAppendTo offset at which to append to to file. 
#'     To let the server choose offset, pass `-1`.
#' @param verbose Print tracing information (default FALSE).
#' @return response object
#' @details Exceptions - IOException
#' 
#' @family Azure Data Lake Store functions
#' 
#' @references \url{https://docs.microsoft.com/en-us/azure/data-lake-store/data-lake-store-data-operations-rest-api#upload-data}
#' @seealso \url{https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Append_to_a_File}
#' @seealso \url{https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Buffer_Size}
#' @seealso \url{https://hadoop.apache.org/docs/current/api/org/apache/hadoop/fs/FileSystem.html#append-org.apache.hadoop.fs.Path-int-org.apache.hadoop.util.Progressable-}
adls.append.core <- function(azureActiveContext, adlFileOutputStream = NULL, azureDataLakeAccount, relativePath, bufferSize, 
                             contents, contentSize = -1L, 
                             leaseId = NULL, sessionId = NULL, syncFlag = NULL, 
                             offsetToAppendTo = -1,
                             verbose = FALSE) {
  if (!missing(azureActiveContext) && !is.null(azureActiveContext)) {
    assert_that(is.azureActiveContext(azureActiveContext))
    azureCheckToken(azureActiveContext)
  }
  assert_that(is_adls_account(azureDataLakeAccount))
  assert_that(is_relativePath(relativePath))
  assert_that(is_bufferSize(bufferSize))
  assert_that(is_content(contents))
  assert_that(is_contentSize(contentSize))
  if (contentSize == -1) {
    contentSize <- getContentSize(contents)
  }
  # allow a zero byte append
  URL <- paste0(
    getAzureDataLakeBasePath(azureDataLakeAccount),
    getAzureDataLakeURLEncodedString(relativePath),
    "?op=APPEND", "&append=true",
    getAzureDataLakeApiVersion()
  )
  if (!missing(bufferSize) && !is.null(bufferSize)) URL <- paste0(URL, "&buffersize=", bufferSize)
  if (!is.null(leaseId)) URL <- paste0(URL, "&leaseid=", leaseId)
  if (!is.null(sessionId)) URL <- paste0(URL, "&filesessionid=", sessionId)
  if (!is.null(syncFlag)) URL <- paste0(URL, "&syncFlag=", syncFlag)
  if (offsetToAppendTo >= 0) URL <- paste0(URL, "&offset=", offsetToAppendTo)
  retryPolicy <- createAdlRetryPolicy(azureActiveContext, verbose = verbose)
  resHttp <- callAzureDataLakeApi(URL, verb = "POST",
                                  azureActiveContext = azureActiveContext,
                                  adlRetryPolicy = retryPolicy,
                                  content = contents[1:contentSize],
                                  verbose = verbose)
  # update retry count - required for bad offset handling
  if (!is.null(adlFileOutputStream)) {
    adlFileOutputStream$numRetries <- retryPolicy$retryCount
  }
  return(resHttp)
}

# ADLS Egress - AdlFileInputStream ----

#' Create an adls.fileinputstream.create
#' Create a container (`adlFileInputStream`) for holding variables used by the Azure Data Lake Store data functions.
#'
#' @inheritParams setAzureContext
#' @param accountName the account name
#' @param relativePath Relative path of a file/directory
#' @param verbose Print tracing information (default FALSE).
#' @return An `adlFileOutputStream` object
#'
#' @family Azure Data Lake Store functions
adls.fileinputstream.create <- function(azureActiveContext, accountName, relativePath, verbose = FALSE) {
  azEnv <- new.env(parent = emptyenv())
  azEnv <- as.adlFileInputStream(azEnv)
  list2env(
    list(azureActiveContext = "", accountName = "", relativePath = ""),
    envir = azEnv
  )
  if (!missing(azureActiveContext)) azEnv$azureActiveContext <- azureActiveContext
  if (!missing(accountName)) azEnv$accountName <- accountName
  if (!missing(relativePath)) azEnv$relativePath <- relativePath
  azEnv$directoryEntry <- adls.file.info(azureActiveContext, accountName, relativePath, verbose)
  if(azEnv$directoryEntry$FileStatus.type == "DIRECTORY") {
    msg <- paste0("ADLException: relativePath is not a file: ", relativePath)
    stop(msg)
  }
  azEnv$sessionId <- uuid()
  azEnv$blockSize <- getAzureDataLakeDefaultBufferSize()
  azEnv$buffer <- raw(0)
  # cursors/indices/offsets in R should start from 1 and NOT 0. 
  # Because of this there are many adjustments that need to be done throughout the code!
  azEnv$fCursor <- 0L # cursor of buffer within file - offset of next byte to read from remote server
  azEnv$bCursor <- 1L # cursor of read within buffer - offset of next byte to be returned from buffer
  azEnv$limit <- 1L # offset of next byte to be read into buffer from service (i.e., upper marker+1 of valid bytes in buffer)
  azEnv$streamClosed <- FALSE
  
  return(azEnv)
}

#' Core function to open and read a file.
#'
#' @inheritParams setAzureContext
#' @param azureDataLakeAccount Name of the Azure Data Lake account.
#' @param relativePath Relative path of a file/directory.
#' @param offset Provide the offset to read from.
#' @param length Provide length of data to read.
#' @param bufferSize Size of the buffer to be used. (not honoured).
#' @param verbose Print tracing information (default FALSE).
#' @return raw contents of the file.
#' @details Exceptions - IOException
#'
#' @family Azure Data Lake Store functions
#'
#' @references \url{https://docs.microsoft.com/en-us/azure/data-lake-store/data-lake-store-data-operations-rest-api#read-data}
#' @seealso \url{https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Open_and_Read_a_File}
#' @seealso \url{https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Offset}
#' @seealso \url{https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Length}
#' @seealso \url{https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Buffer_Size}
#' @seealso \url{https://hadoop.apache.org/docs/current/api/org/apache/hadoop/fs/FileSystem.html#open-org.apache.hadoop.fs.Path-int-}
adls.read.core <- function(azureActiveContext, 
                           azureDataLakeAccount, relativePath, 
                           offset, length, bufferSize = 4194304L, 
                           verbose = FALSE) {
  if (!missing(azureActiveContext) && !is.null(azureActiveContext)) {
    assert_that(is.azureActiveContext(azureActiveContext))
    azureCheckToken(azureActiveContext)
  }
  assert_that(is_adls_account(azureDataLakeAccount))
  assert_that(is_relativePath(relativePath))
  if (!missing(offset) && !is.null(offset)) assert_that(is_offset(offset))
  if (!missing(length) && !is.null(length)) assert_that(is_length(length))
  if (!missing(bufferSize) && !is.null(bufferSize)) assert_that(is_bufferSize(bufferSize))
  URL <- paste0(
    getAzureDataLakeBasePath(azureDataLakeAccount),
    getAzureDataLakeURLEncodedString(relativePath),
    "?op=OPEN", "&read=true",
    getAzureDataLakeApiVersion()
  )
  if (!missing(offset) && !is.null(offset)) URL <- paste0(URL, "&offset=", offset)
  if (!missing(length) && !is.null(length)) URL <- paste0(URL, "&length=", length)
  if (!missing(bufferSize) && !is.null(bufferSize)) URL <- paste0(URL, "&buffersize=", bufferSize)
  retryPolicy <- createAdlRetryPolicy(azureActiveContext, verbose = verbose)
  resHttp <- callAzureDataLakeApi(URL,
                                  azureActiveContext = azureActiveContext,
                                  adlRetryPolicy = retryPolicy,
                                  verbose = verbose)
  return(resHttp)
}

#' Read from service attempts to read `blocksize` bytes from service.
#' Returns how many bytes are actually read, could be less than blocksize.
#'
#' @param adlFileInputStream the `adlFileInputStream` object to read from
#' @param verbose Print tracing information (default FALSE)
#' @return number of bytes actually read
#' 
#' @family Azure Data Lake Store functions
adls.fileinputstream.readfromservice <- function(adlFileInputStream, verbose = FALSE) {
  if (adlFileInputStream$bCursor < adlFileInputStream$limit) return(0) #if there's still unread data in the buffer then dont overwrite it At or past end of file
  if (adlFileInputStream$fCursor >= adlFileInputStream$directoryEntry$FileStatus.length) return(-1)
  if (adlFileInputStream$directoryEntry$FileStatus.length <= adlFileInputStream$blockSize)
    return(adls.fileinputstream.slurpfullfile(adlFileInputStream))
  
  #reset buffer to initial state - i.e., throw away existing data
  adlFileInputStream$bCursor <- 1L
  adlFileInputStream$limit <- 1L
  if (is.null(adlFileInputStream$buffer)) adlFileInputStream$buffer <- raw(getAzureDataLakeDefaultBufferSize())
  
  resHttp <- adls.read.core(adlFileInputStream$azureActiveContext, 
                            adlFileInputStream$accountName, adlFileInputStream$relativePath, 
                            adlFileInputStream$fCursor, adlFileInputStream$blockSize, 
                            verbose = verbose)
  stopWithAzureError(resHttp)
  data <- content(resHttp, "raw", encoding = "UTF-8")
  bytesRead <- getContentSize(data)
  adlFileInputStream$buffer[1:bytesRead] <- data[1:bytesRead]
  adlFileInputStream$limit <- adlFileInputStream$limit + bytesRead
  adlFileInputStream$fCursor <- adlFileInputStream$fCursor + bytesRead
  return(bytesRead)
}

#' Reads the whole file into buffer. Useful when reading small files.
#'
#' @param adlFileInputStream the adlFileInputStream object to read from
#' @param verbose Print tracing information (default FALSE)
#' @return number of bytes actually read
adls.fileinputstream.slurpfullfile <- function(adlFileInputStream, verbose = FALSE) {
  if (is.null(adlFileInputStream$buffer)) {
    adlFileInputStream$blocksize <- adlFileInputStream$directoryEntry$FileStatus.length
    adlFileInputStream$buffer <- raw(adlFileInputStream$directoryEntry$FileStatus.length)
  }
  
  #reset buffer to initial state - i.e., throw away existing data
  adlFileInputStream$bCursor <- adls.fileinputstream.getpos(adlFileInputStream) + 1L  # preserve current file offset (may not be 0 if app did a seek before first read)
  adlFileInputStream$limit <- 1L
  adlFileInputStream$fCursor <- 0L  # read from beginning
  
  resHttp <- adls.read.core(adlFileInputStream$azureActiveContext, 
                            adlFileInputStream$accountName, adlFileInputStream$relativePath, 
                            adlFileInputStream$fCursor, adlFileInputStream$directoryEntry$FileStatus.length, 
                            verbose = verbose)
  stopWithAzureError(resHttp)
  data <- content(resHttp, "raw", encoding = "UTF-8")
  bytesRead <- getContentSize(data)
  adlFileInputStream$buffer[1:bytesRead] <- data[1:bytesRead]
  adlFileInputStream$limit <- adlFileInputStream$limit + bytesRead
  adlFileInputStream$fCursor <- adlFileInputStream$fCursor + bytesRead
  return(bytesRead)
}

# ADLS Retry Policies ----

#' NOTE: Folowing points on ADLS AdlsRetryPolicy:
#' 1. Not implemented speculative reads hence not implemented `NoRetryPolicy`.
#' 2. Not implemented ExponentialBackoffPolicyforMSI as its not used even in the JDK.

#' Create adlRetryPolicy.
#' Create a adlRetryPolicy (`adlRetryPolicy`) for holding variables used by the Azure Data Lake Store data functions.
#'
#' @inheritParams setAzureContext
#' @param retryPolicyType the type of retryPlociy object to create.
#' @param verbose Print tracing information (default FALSE).
#' @return An `adlRetryPolicy` object
#'
#' @family Azure Data Lake Store functions
#'
#' @references \url{https://github.com/Azure/azure-data-lake-store-java/blob/master/src/main/java/com/microsoft/azure/datalake/store/retrypolicies/RetryPolicy.java}
createAdlRetryPolicy <- function(azureActiveContext, retryPolicyType = retryPolicyEnum$EXPONENTIALBACKOFF, verbose = FALSE) {
  azEnv <- new.env(parent = emptyenv())
  azEnv <- as.adlRetryPolicy(azEnv)
  list2env(
    list(azureActiveContext = ""),
    envir = azEnv
  )
  if (!missing(azureActiveContext)) azEnv$azureActiveContext <- azureActiveContext
  # init the azEnv (adlRetryPolicy) with the right params
  azEnv$retryPolicyType <- retryPolicyType
  if(retryPolicyType == retryPolicyEnum$EXPONENTIALBACKOFF) {
    return(createAdlExponentialBackoffRetryPolicy(azEnv, verbose))
  } else if(retryPolicyType == retryPolicyEnum$NONIDEMPOTENT) {
    return(createAdlNonIdempotentRetryPolicy(azEnv, verbose))
  } else {
    printADLSMessage("internal.R", "createAdlRetryPolicy", 
                     paste0("UndefinedRetryPolicyTypeError: ", azEnv$retryPolicyType),
                     NULL)
    return(NULL)
  }
}

#' Create an adlExponentialBackoffRetryPolicy.
#'
#' @param adlRetryPolicy the retrypolicy object to initialize.
#' @param verbose Print tracing information (default FALSE).
#' @return An `adlRetryPolicy` object
#'
#' @family Azure Data Lake Store functions
#'
#' @references \url{https://github.com/Azure/azure-data-lake-store-java/blob/master/src/main/java/com/microsoft/azure/datalake/store/retrypolicies/ExponentialBackoffPolicy.java}
createAdlExponentialBackoffRetryPolicy <- function(adlRetryPolicy, verbose = FALSE) {
  adlRetryPolicy$retryCount <- 0
  adlRetryPolicy$maxRetries <- 4
  adlRetryPolicy$exponentialRetryInterval <- 1000 # in milliseconds
  adlRetryPolicy$exponentialFactor <- 4
  adlRetryPolicy$lastAttemptStartTime <- getCurrentTimeInNanos() # in nanoseconds
  return(adlRetryPolicy)
}

#' Create an adlNonIdempotentRetryPolicy.
#'
#' @param adlRetryPolicy the retrypolicy object to initialize.
#' @param verbose Print tracing information (default FALSE).
#' @return An `adlRetryPolicy` object
#'
#' @family Azure Data Lake Store functions
#'
#' @references \url{https://github.com/Azure/azure-data-lake-store-java/blob/master/src/main/java/com/microsoft/azure/datalake/store/retrypolicies/NonIdempotentRetryPolicy.java}
createAdlNonIdempotentRetryPolicy <- function(adlRetryPolicy, verbose = FALSE) {
  adlRetryPolicy$retryCount401 <- 0
  adlRetryPolicy$waitInterval <- 100
  adlRetryPolicy$retryCount429 <- 0
  adlRetryPolicy$maxRetries <- 4
  adlRetryPolicy$exponentialRetryInterval <- 1000 # in milliseconds
  adlRetryPolicy$exponentialFactor <- 4
  return(adlRetryPolicy)
}

#' Check if retry should be done based on `adlRetryPolicy`.
#'
#' @param adlRetryPolicy the policy object to chek for retry
#' @param httpResponseCode the account name
#' @param lastException exception that was reported with failure
#' @param verbose Print tracing information (default FALSE)
#' @return TRUE for retry and FALSE otherwise
#'
#' @family Azure Data Lake Store functions
shouldRetry <- function(adlRetryPolicy, 
                        httpResponseCode, lastException, 
                        verbose = FALSE) {
  if(adlRetryPolicy$retryPolicyType == retryPolicyEnum$EXPONENTIALBACKOFF) {
    return(
      shouldRetry.adlExponentialBackoffRetryPolicy(
        adlRetryPolicy, httpResponseCode, lastException, verbose))
  } else if(adlRetryPolicy$retryPolicyType == retryPolicyEnum$NONIDEMPOTENT) {
    return(
      shouldRetry.adlNonIdempotentRetryPolicy(
        adlRetryPolicy, httpResponseCode, lastException, verbose))
  } else {
    printADLSMessage("internal.R", "shouldRetry", 
                     paste0("UndefinedRetryPolicyTypeError: ", adlRetryPolicy$retryPolicyType),
                     NULL)
    return(NULL)
  }
}

#' Check if retry should be done based on `adlRetryPolicy` (adlExponentialBackoffRetryPolicy).
#'
#' @param adlRetryPolicy the policy object to chek for retry
#' @param httpResponseCode the account name
#' @param lastException exception that was reported with failure
#' @param verbose Print tracing information (default FALSE)
#' @return TRUE for retry and FALSE otherwise
#'
#' @family Azure Data Lake Store functions
shouldRetry.adlExponentialBackoffRetryPolicy <- function(adlRetryPolicy, 
                        httpResponseCode, lastException, 
                        verbose = FALSE) {
  if (missing(adlRetryPolicy) || missing(httpResponseCode)) {
    return(FALSE)
  }
  # Non-retryable error
  if ((
    httpResponseCode >= 300 && httpResponseCode < 500 # 3xx and 4xx, except specific ones below
    && httpResponseCode != 408
    && httpResponseCode != 429
    && httpResponseCode != 401
  )
  || (httpResponseCode == 501) # Not Implemented
  || (httpResponseCode == 505) # Version Not Supported
  ) {
    return(FALSE)
  }
  # Retryable error, retry with exponential backoff
  if (!is.null(lastException)
      || httpResponseCode >= 500 # exception or 5xx, + specific ones below
      || httpResponseCode == 408
      || httpResponseCode == 429
      || httpResponseCode == 401) {
    if (adlRetryPolicy$retryCount < adlRetryPolicy$maxRetries) {
      timeSpentInMillis <- as.integer((getCurrentTimeInNanos() - adlRetryPolicy$lastAttemptStartTime) / 1000000)
      wait(adlRetryPolicy$exponentialRetryInterval - timeSpentInMillis)
      adlRetryPolicy$exponentialRetryInterval <- (adlRetryPolicy$exponentialRetryInterval * adlRetryPolicy$exponentialFactor)
      adlRetryPolicy$retryCount <- adlRetryPolicy$retryCount + 1
      adlRetryPolicy$lastAttemptStartTime <- getCurrentTimeInNanos()
      return(TRUE)
    } else {
      return(FALSE) # max # of retries exhausted
    }
  }
  # these are not errors - this method should never have been called with this
  if (httpResponseCode >= 100 && httpResponseCode < 300) {
    return(FALSE)
  }
  # Dont know what happened - we should never get here
  return(FALSE)
}

#' Check if retry should be done based on `adlRetryPolicy` (adlNonIdempotentRetryPolicy).
#'
#' @param adlRetryPolicy the policy object to chek for retry
#' @param httpResponseCode the account name
#' @param lastException exception that was reported with failure
#' @param verbose Print tracing information (default FALSE)
#' @return TRUE for retry and FALSE otherwise
#'
#' @family Azure Data Lake Store functions
shouldRetry.adlNonIdempotentRetryPolicy <- function(adlRetryPolicy,
                                                    httpResponseCode, lastException, 
                                                    verbose = FALSE) {
  if (httpResponseCode == 401 && adlRetryPolicy$retryCount401 == 0) {
    # this could be because of call delay. Just retry once, in hope of token being renewed by now
    wait(adlRetryPolicy$waitInterval)
    adlRetryPolicy$retryCount401 <- (adlRetryPolicy$retryCount401 + 1)
    return(TRUE)
  }

  if (httpResponseCode == 429) {
    # 429 means that the backend did not change any state.
    if (adlRetryPolicy$retryCount429 < adlRetryPolicy$maxRetries) {
      wait(adlRetryPolicy$exponentialRetryInterval)
      adlRetryPolicy$exponentialRetryInterval <- (adlRetryPolicy$exponentialRetryInterval * adlRetryPolicy$exponentialFactor)
      adlRetryPolicy$retryCount429 <- (adlRetryPolicy$retryCount429 + 1)
      return(TRUE)
    } else {
      return(FALSE)  # max # of retries exhausted
    }
  }

  return(FALSE)
}

wait <- function(waitTimeInMilliSeconds, verbose = FALSE) {
  if (waitTimeInMilliSeconds <= 0) {
    return(NULL)
  }
  tryCatch(
    {
      if(verbose) {
        printADLSMessage("internal.R", "wait", 
                         paste0("going into wait for waitTimeInMilliSeconds=", waitTimeInMilliSeconds),
                         NULL)
      }
      Sys.sleep(waitTimeInMilliSeconds/1000)
    }, interrupt = function(e) {
      if (verbose) {
        printADLSMessage("internal.R", "wait", "interrupted while wait during retry", e)
      }
    }, error = function(e) {
      if (verbose) {
        printADLSMessage("internal.R", "wait", "error while wait during retry", e)
      }
    }
  )
  return(NULL)
}

isSuccessfulResponse <- function(resHttp, op) {
  #if (http_error(resHttp)) return(FALSE)
  #if (http_status(resHttp)$category != "Success") return(FALSE)
  if (status_code(resHttp) >= 100 && status_code(resHttp) < 300) return(TRUE) # 1xx and 2xx return codes
  return(FALSE) # anything else
}

# ADLS Rest Calls ----

callAzureDataLakeApi <- function(url, verb = "GET", azureActiveContext, adlRetryPolicy = NULL,
                                 content = raw(0), contenttype = NULL, #"application/octet-stream",
                                 verbose = FALSE) {
  resHttp <- NULL
  repeat {
    resHttp <- callAzureDataLakeRestEndPoint(url, verb, azureActiveContext,
                                  content, contenttype,
                                  verbose)
    if (!isSuccessfulResponse(resHttp) 
        && shouldRetry(adlRetryPolicy, status_code(resHttp), NULL)) {
      if (verbose) {
        msg <- paste0("retry request: "
                      , " status=", http_status(resHttp)$message
                      , ", url=", url, ", verb=", verb
                      , ", adlsRetryPolicy=", as.character.adlRetryPolicy(adlRetryPolicy))
        printADLSMessage("internal.R", "callAzureDataLakeApi", msg, NULL)
      }
      next # continue trying till succeeded or retries exceeded
    } else {
      break # break on success or all planned retries failed
    }
  }
  return(resHttp)
}

callAzureDataLakeRestEndPoint <- function(url, verb = "GET", azureActiveContext,
                                 content = raw(0), contenttype = NULL, #"application/octet-stream",
                                 verbose = FALSE) {
  verbosity <- set_verbosity(verbose)
  commonHeaders <- c(Authorization = azureActiveContext$Token
                     , `User-Agent` = getAzureDataLakeSDKUserAgent()
                     , `x-ms-client-request-id` = uuid()
  )
  resHttp <- switch(verb,
                    "GET" = GET(url,
                                add_headers(.headers = c(commonHeaders
                                                         , `Content-Length` = "0"
                                )
                                ),
                                verbosity
                    ),
                    "PUT" = PUT(url,
                                add_headers(.headers = c(commonHeaders
                                                         #, `Transfer-Encoding` = "chunked"
                                                         , `Content-Length` = getContentSize(content)
                                                         , `Content-Type` = contenttype
                                )
                                ),
                                body = content,
                                verbosity
                    ),
                    "POST" = POST(url,
                                  add_headers(.headers = c(commonHeaders
                                                           #, `Transfer-Encoding` = "chunked"
                                                           , `Content-Length` = getContentSize(content)
                                                           , `Content-Type` = contenttype
                                  )
                                  ),
                                  body = content,
                                  verbosity
                    ),
                    "DELETE" = DELETE(url,
                                      add_headers(.headers = c(commonHeaders
                                                               , `Content-Length` = "0"
                                      )
                                      ),
                                      verbosity
                    )
  )
  # Print the response body in case verbose is enabled.
  if (verbose) {
    resJsonStr <- content(resHttp, "text", encoding = "UTF-8")
    printADLSMessage("internal.R", "callAzureDataLakeRestEndPoint", resJsonStr, NULL)
  }
  return(resHttp)
}
