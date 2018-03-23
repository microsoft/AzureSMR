#' List the statuses of the files/directories in the given path.
#'
#' @inheritParams setAzureContext
#' @param azureDataLakeAccount Name of the Azure Data Lake Store account.
#' @param relativePath Relative path of a file/directory.
#' @param verbose Print tracing information (default FALSE).
#' @return Returns a FileStatuses data frame with a row for each directory element. 
#'         Each row has 11 columns (12 in case of a file):
#'         FileStatuses.FileStatus.(accessTime, modificationTime, replication, permission, owner, group, aclBit, msExpirationtime (in case of file))
#' Exception FileNotFoundException
#' Exception IOException
#'
#' @family Azure Data Lake Store functions
#' @export
#'
#' @references \url{https://docs.microsoft.com/en-us/azure/data-lake-store/data-lake-store-data-operations-rest-api#list-folders}
#' @seealso \url{https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#List_a_Directory}
#' @seealso \url{https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#List_a_File}
#' @seealso \url{https://hadoop.apache.org/docs/current/api/org/apache/hadoop/fs/FileSystem.html#listStatus-org.apache.hadoop.fs.Path-}
azureDataLakeListStatus <- function(azureActiveContext, azureDataLakeAccount, relativePath, verbose = FALSE) {
  if (!missing(azureActiveContext) && !is.null(azureActiveContext)) {
    assert_that(is.azureActiveContext(azureActiveContext))
    azureCheckToken(azureActiveContext)
  }
  assert_that(is_storage_account(azureDataLakeAccount))
  assert_that(is_relativePath(relativePath))
  URL <- paste0(
    getAzureDataLakeBasePath(azureDataLakeAccount),
    getAzureDataLakeURLEncodedString(relativePath),
    "?op=LISTSTATUS",
    getAzureDataLakeApiVersion()
    )
  resHttp <- callAzureDataLakeApi(URL,
                                  azureActiveContext = azureActiveContext,
                                  verbose = verbose)
  stopWithAzureError(resHttp)
  resJsonStr <- content(resHttp, "text", encoding = "UTF-8")
  resJsonObj <- jsonlite::fromJSON(resJsonStr)
  if (length(resJsonObj$FileStatuses$FileStatus) == 0) {
    # Return empty data frame in case of an empty directory
    return(
      data.frame(
        FileStatuses.FileStatus.length = character(0),
        FileStatuses.FileStatus.pathSuffix = character(0),
        FileStatuses.FileStatus.type = character(0),
        FileStatuses.FileStatus.blockSize = character(0),
        FileStatuses.FileStatus.accessTime = character(0),
        FileStatuses.FileStatus.modificationTime = character(0),
        FileStatuses.FileStatus.replication = character(0),
        FileStatuses.FileStatus.permission = character(0),
        FileStatuses.FileStatus.owner = character(0),
        FileStatuses.FileStatus.group = character(0),
        # NOTE: There is an additional msExpirationTime for files
        FileStatuses.FileStatus.aclBit = character(0)
      )
    )
  }
  resDf <- as.data.frame(resJsonObj)
  return(resDf)
}

#' Get the status of a file/directory in the given path.
#'
#' @inheritParams setAzureContext
#' @param azureDataLakeAccount Name of the Azure Data Lake account.
#' @param relativePath Relative path of a file/directory.
#' @param verbose Print tracing information (default FALSE).
#' @return Returns a FileStatus data frame with one row for directory element. 
#'         The row has 11 columns (12 in case of a [file]):
#'         FileStatuses.FileStatus.(accessTime, modificationTime, replication, permission, owner, group, aclBit, msExpirationtime (in case of file))
#' Exception FileNotFoundException
#' Exception IOException
#'
#' @family Azure Data Lake Store functions
#' @export
#'
#' @references \url{https://docs.microsoft.com/en-us/azure/data-lake-store/data-lake-store-data-operations-rest-api}
#' @seealso \url{https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Status_of_a_File.2FDirectory}
#' @seealso \url{https://hadoop.apache.org/docs/current/api/org/apache/hadoop/fs/FileSystem.html#getFileStatus-org.apache.hadoop.fs.Path-}
azureDataLakeGetFileStatus <- function(azureActiveContext, azureDataLakeAccount, relativePath, verbose = FALSE) {
  if (!missing(azureActiveContext) && !is.null(azureActiveContext)) {
    assert_that(is.azureActiveContext(azureActiveContext))
    azureCheckToken(azureActiveContext)
  }
  assert_that(is_storage_account(azureDataLakeAccount))
  assert_that(is_relativePath(relativePath))
  URL <- paste0(
    getAzureDataLakeBasePath(azureDataLakeAccount),
    getAzureDataLakeURLEncodedString(relativePath),
    "?op=GETFILESTATUS",
    getAzureDataLakeApiVersion()
  )
  resHttp <- callAzureDataLakeApi(URL,
                                  azureActiveContext = azureActiveContext,
                                  verbose = verbose)
  stopWithAzureError(resHttp)
  resJsonStr <- content(resHttp, "text", encoding = "UTF-8")
  resJsonObj <- jsonlite::fromJSON(resJsonStr)
  if (length(resJsonObj$FileStatus) == 0) {
    # Return empty data frame in case of an empty directory
    return(
      data.frame(
        FileStatuses.FileStatus.length = character(0),
        FileStatuses.FileStatus.pathSuffix = character(0),
        FileStatuses.FileStatus.type = character(0),
        FileStatuses.FileStatus.blockSize = character(0),
        FileStatuses.FileStatus.accessTime = character(0),
        FileStatuses.FileStatus.modificationTime = character(0),
        FileStatuses.FileStatus.replication = character(0),
        FileStatuses.FileStatus.permission = character(0),
        FileStatuses.FileStatus.owner = character(0),
        FileStatuses.FileStatus.group = character(0),
        # NOTE: There is an additional msExpirationTime for files
        FileStatuses.FileStatus.aclBit = character(0)
      )
    )
  }
  resDf <- as.data.frame(resJsonObj)
  return(resDf)
}

#' Create a directory with the provided permission.
#'
#' @inheritParams setAzureContext
#' @param azureDataLakeAccount Name of the Azure Data Lake account.
#' @param relativePath Relative path of a file/directory.
#' @param permission Permission to be set for the directory (default is 755).
#' @param verbose Print tracing information (default FALSE).
#' @return Returns true if the directory creation succeeds; false otherwise.
#' Exception IOException
#'
#' @family Azure Data Lake Store functions
#' @export
#'
#' @references \url{https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Make_a_Directory}
#' @seealso \url{https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Permission}
#' @seealso \url{https://hadoop.apache.org/docs/current/api/org/apache/hadoop/fs/FileSystem.html#mkdirs-org.apache.hadoop.fs.FileSystem-org.apache.hadoop.fs.Path-org.apache.hadoop.fs.permission.FsPermission-}
azureDataLakeMkdirs <- function(azureActiveContext, azureDataLakeAccount, relativePath, permission, verbose = FALSE) {
  if (!missing(azureActiveContext) && !is.null(azureActiveContext)) {
    assert_that(is.azureActiveContext(azureActiveContext))
    azureCheckToken(azureActiveContext)
  }
  assert_that(is_storage_account(azureDataLakeAccount))
  assert_that(is_relativePath(relativePath))
  if (!missing(permission) && !is.null(permission)) assert_that(is_permission(permission))
  URL <- paste0(
    getAzureDataLakeBasePath(azureDataLakeAccount),
    getAzureDataLakeURLEncodedString(relativePath),
    "?op=MKDIRS",
    getAzureDataLakeApiVersion()
  )
  if (!missing(permission) && !is.null(permission)) URL <- paste0(URL, "&permission=", permission)
  resHttp <- callAzureDataLakeApi(URL, verb = "PUT",
                                  azureActiveContext = azureActiveContext,
                                  verbose = verbose)
  stopWithAzureError(resHttp)
  resJsonStr <- content(resHttp, "text", encoding = "UTF-8")
  resJsonObj <- jsonlite::fromJSON(resJsonStr)
  resDf <- as.data.frame(resJsonObj)
  return(resDf$boolean)
}

#' Create and write to a file.
#'
#' @inheritParams setAzureContext
#' @param azureDataLakeAccount Name of the Azure Data Lake account.
#' @param relativePath Relative path of a file.
#' @param permission Permission to be set for file (default 644).
#' @param overwrite Whether to overwrite existing files with same name (default FALSE).
#' @param bufferSize Size of the buffer to be used.
#' @param replication Required block replication for a file.
#' @param blockSize Block size of the file.
#' @param contents raw contents to be written to the newly created file (default raw(0)).
#' @param verbose Print tracing information (default FALSE).
#' @return NULL (void)
#' Exception IOException
#'
#' @family Azure Data Lake Store functions
#' @export
#'
#' @references \url{https://docs.microsoft.com/en-us/azure/data-lake-store/data-lake-store-data-operations-rest-api#upload-data}
#' @seealso \url{https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Create_and_Write_to_a_File}
#' @seealso \url{https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Overwrite}
#' @seealso \url{https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Block_Size}
#' @seealso \url{https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Replication}
#' @seealso \url{https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Permission}
#' @seealso \url{https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Buffer_Size}
#' @seealso \url{https://hadoop.apache.org/docs/current/api/org/apache/hadoop/fs/FileSystem.html#create-org.apache.hadoop.fs.Path-org.apache.hadoop.fs.permission.FsPermission-boolean-int-short-long-org.apache.hadoop.util.Progressable-}
azureDataLakeCreate <- function(azureActiveContext, azureDataLakeAccount, relativePath,
                                permission, overwrite = FALSE,
                                bufferSize, replication, blockSize,
                                contents = raw(0), verbose = FALSE) {
  if (!missing(azureActiveContext) && !is.null(azureActiveContext)) {
    assert_that(is.azureActiveContext(azureActiveContext))
    azureCheckToken(azureActiveContext)
  }
  assert_that(is_storage_account(azureDataLakeAccount))
  assert_that(is_relativePath(relativePath))
  if (!missing(permission) && !is.null(permission)) assert_that(is_permission(permission))
  if (!missing(bufferSize) && !is.null(bufferSize)) assert_that(is_bufferSize(bufferSize))
  if (!missing(replication) && !is.null(replication)) assert_that(is_replication(replication))
  if (!missing(blockSize) && !is.null(blockSize)) assert_that(is_blockSize(blockSize))
  if (!missing(contents) && !is.null(contents)) assert_that(is_content(contents))
  URL <- paste0(
    getAzureDataLakeBasePath(azureDataLakeAccount),
    getAzureDataLakeURLEncodedString(relativePath),
    "?op=CREATE", "&write=true",
    getAzureDataLakeApiVersion()
  )
  if (!missing(permission) && !is.null(permission)) URL <- paste0(URL, "&permission=", permission)
  if (!missing(overwrite) && !is.null(overwrite)) URL <- paste0(URL, "&overwrite=", overwrite)
  if (!missing(bufferSize) && !is.null(bufferSize)) URL <- paste0(URL, "&buffersize=", bufferSize)
  if (!missing(replication) && !is.null(replication)) URL <- paste0(URL, "&replication=", replication)
  if (!missing(blockSize) && !is.null(blockSize)) URL <- paste0(URL, "&blocksize=", blockSize)
  resHttp <- callAzureDataLakeApi(URL, verb = "PUT",
                                  azureActiveContext = azureActiveContext,
                                  content = contents,
                                  verbose = verbose)
  stopWithAzureError(resHttp)
  # return a NULL (void)
  return(NULL)
}

#' Append to an existing file.
#'
#' @inheritParams setAzureContext
#' @param azureDataLakeAccount Name of the Azure Data Lake account.
#' @param relativePath Relative path of a file.
#' @param bufferSize Size of the buffer to be used.
#' @param contents raw contents to be written to the file.
#' @param contentSize size of `contents` to be written to the file.
#' @param verbose Print tracing information (default FALSE).
#' @return NULL (void)
#' Exception IOException
#' 
#' @family Azure Data Lake Store functions
#' @export
#' 
#' @references \url{https://docs.microsoft.com/en-us/azure/data-lake-store/data-lake-store-data-operations-rest-api#upload-data}
#' @seealso \url{https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Append_to_a_File}
#' @seealso \url{https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Buffer_Size}
#' @seealso \url{https://hadoop.apache.org/docs/current/api/org/apache/hadoop/fs/FileSystem.html#append-org.apache.hadoop.fs.Path-int-org.apache.hadoop.util.Progressable-}
azureDataLakeAppend <- function(azureActiveContext, azureDataLakeAccount, relativePath, 
                                bufferSize, contents, contentSize = -1L, verbose = FALSE) {
  resHttp <- azureDataLakeAppendCore(azureActiveContext, azureDataLakeAccount, relativePath,
                                     bufferSize, contents, contentSize, verbose = verbose)
  stopWithAzureError(resHttp)
  # retrun a NULL (void)
  return(NULL)
}

#' AppendBOS to an existing file.
#'
#' @inheritParams setAzureContext
#' @param azureDataLakeAccount Name of the Azure Data Lake account.
#' @param relativePath Relative path of a file.
#' @param verbose Print tracing information (default FALSE).
#' @return adlFileOutputStream object.
#' Exception IOException
#'
#' @family Azure Data Lake Store functions
#' @export
#'
#' @references \url{https://docs.microsoft.com/en-us/azure/data-lake-store/data-lake-store-data-operations-rest-api#upload-data}
#' @seealso \url{https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Append_to_a_File}
#' @seealso \url{https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Buffer_Size}
#' @seealso \url{https://hadoop.apache.org/docs/current/api/org/apache/hadoop/fs/FileSystem.html#append-org.apache.hadoop.fs.Path-int-org.apache.hadoop.util.Progressable-}
azureDataLakeAppendBOS <- function(azureActiveContext, azureDataLakeAccount, relativePath, verbose = FALSE) {
  if (!missing(azureActiveContext) && !is.null(azureActiveContext)) {
    assert_that(is.azureActiveContext(azureActiveContext))
    azureCheckToken(azureActiveContext)
  }
  assert_that(is_storage_account(azureDataLakeAccount))
  assert_that(is_relativePath(relativePath))
  adlFOS <- createAdlFileOutputStream(azureActiveContext, azureDataLakeAccount, relativePath, verbose)
  return(adlFOS)
}

#' The Core Append API.
#'
#' @inheritParams setAzureContext
#' @param azureDataLakeAccount Name of the Azure Data Lake account.
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
#'         retrieved from `azureDataLakeGetfileStatus` or `azureDatalakeListStatus API call.
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
#' Exception IOException
#' 
#' @family Azure Data Lake Store functions
#' 
#' @references \url{https://docs.microsoft.com/en-us/azure/data-lake-store/data-lake-store-data-operations-rest-api#upload-data}
#' @seealso \url{https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Append_to_a_File}
#' @seealso \url{https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Buffer_Size}
#' @seealso \url{https://hadoop.apache.org/docs/current/api/org/apache/hadoop/fs/FileSystem.html#append-org.apache.hadoop.fs.Path-int-org.apache.hadoop.util.Progressable-}
azureDataLakeAppendCore <- function(azureActiveContext, azureDataLakeAccount, relativePath, bufferSize, 
                                    contents, contentSize = -1L, 
                                    leaseId = NULL, sessionId = NULL, syncFlag = NULL, 
                                    offsetToAppendTo = -1,
                                    verbose = FALSE) {
  if (!missing(azureActiveContext) && !is.null(azureActiveContext)) {
    assert_that(is.azureActiveContext(azureActiveContext))
    azureCheckToken(azureActiveContext)
  }
  assert_that(is_storage_account(azureDataLakeAccount))
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
  resHttp <- callAzureDataLakeApi(URL, verb = "POST",
                                  azureActiveContext = azureActiveContext,
                                  content = contents[1:contentSize],
                                  verbose = verbose)
  return(resHttp)
}

#' Open and read a file.
#'
#' @inheritParams setAzureContext
#' @param azureDataLakeAccount Name of the Azure Data Lake account.
#' @param relativePath Relative path of a file/directory.
#' @param offset Provide the offset to read from.
#' @param length Provide length of data to read.
#' @param bufferSize Size of the buffer to be used. (not honoured).
#' @param verbose Print tracing information (default FALSE).
#' @return raw contents of the file.
#' Exception IOException
#'
#' @family Azure Data Lake Store functions
#' @export
#'
#' @references \url{https://docs.microsoft.com/en-us/azure/data-lake-store/data-lake-store-data-operations-rest-api#read-data}
#' @seealso \url{https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Open_and_Read_a_File}
#' @seealso \url{https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Offset}
#' @seealso \url{https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Length}
#' @seealso \url{https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Buffer_Size}
#' @seealso \url{https://hadoop.apache.org/docs/current/api/org/apache/hadoop/fs/FileSystem.html#open-org.apache.hadoop.fs.Path-int-}
azureDataLakeRead <- function(azureActiveContext, 
                              azureDataLakeAccount, relativePath, 
                              offset, length, bufferSize, 
                              verbose = FALSE) {
  resHttp <- azureDataLakeReadCore(azureActiveContext, 
                                   azureDataLakeAccount, relativePath, 
                                   offset, length, bufferSize, 
                                   verbose)
  stopWithAzureError(resHttp)
  resRaw <- content(resHttp, "raw", encoding = "UTF-8")
  return(resRaw)
}

#' Open a file and return an adlFileInputStream
#'
#' @inheritParams setAzureContext
#' @param azureDataLakeAccount Name of the Azure Data Lake account.
#' @param relativePath Relative path of a file/directory.
#' @param bufferSize Size of the buffer to be used. (not honoured).
#' @param verbose Print tracing information (default FALSE).
#' @return an object of adlFileInputStream.
#' Exception IOException
#'
#' @family Azure Data Lake Store functions
#' @export
#'
#' @references \url{https://docs.microsoft.com/en-us/azure/data-lake-store/data-lake-store-data-operations-rest-api#read-data}
#' @seealso \url{https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Open_and_Read_a_File}
#' @seealso \url{https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Offset}
#' @seealso \url{https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Length}
#' @seealso \url{https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Buffer_Size}
#' @seealso \url{https://hadoop.apache.org/docs/current/api/org/apache/hadoop/fs/FileSystem.html#open-org.apache.hadoop.fs.Path-int-}
azureDataLakeOpenBIS <- function(azureActiveContext, azureDataLakeAccount, 
                                 relativePath, bufferSize, 
                                 verbose = FALSE) {
  if (!missing(azureActiveContext) && !is.null(azureActiveContext)) {
    assert_that(is.azureActiveContext(azureActiveContext))
    azureCheckToken(azureActiveContext)
  }
  assert_that(is_storage_account(azureDataLakeAccount))
  assert_that(is_relativePath(relativePath))
  if (!missing(bufferSize) && !is.null(bufferSize)) assert_that(is_bufferSize(bufferSize))
  adlFIS <- createAdlFileInputStream(azureActiveContext, azureDataLakeAccount, relativePath, verbose)

  return(adlFIS)
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
#' Exception IOException
#'
#' @family Azure Data Lake Store functions
#'
#' @references \url{https://docs.microsoft.com/en-us/azure/data-lake-store/data-lake-store-data-operations-rest-api#read-data}
#' @seealso \url{https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Open_and_Read_a_File}
#' @seealso \url{https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Offset}
#' @seealso \url{https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Length}
#' @seealso \url{https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Buffer_Size}
#' @seealso \url{https://hadoop.apache.org/docs/current/api/org/apache/hadoop/fs/FileSystem.html#open-org.apache.hadoop.fs.Path-int-}
azureDataLakeReadCore <- function(azureActiveContext, 
                                  azureDataLakeAccount, relativePath, 
                                  offset, length, bufferSize = 4194304L, 
                                  verbose = FALSE) {
  if (!missing(azureActiveContext) && !is.null(azureActiveContext)) {
    assert_that(is.azureActiveContext(azureActiveContext))
    azureCheckToken(azureActiveContext)
  }
  assert_that(is_storage_account(azureDataLakeAccount))
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
  resHttp <- callAzureDataLakeApi(URL,
                                  azureActiveContext = azureActiveContext,
                                  verbose = verbose)
  return(resHttp)
}

#' Delete a file/directory.
#'
#' @inheritParams setAzureContext
#' @param azureDataLakeAccount Name of the Azure Data Lake account.
#' @param relativePath Relative path of a file/directory.
#' @param recursive If path is a directory, recursively delete contents and directory (default FALSE).
#' @param verbose Print tracing information (default FALSE).
#' @return true if delete is successful else false.
#' Exception IOException
#'
#' @family Azure Data Lake Store functions
#' @export
#'
#' @references \url{https://docs.microsoft.com/en-us/azure/data-lake-store/data-lake-store-data-operations-rest-api#delete-a-file}
#' @seealso \url{https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Recursive}
#' @seealso \url{https://hadoop.apache.org/docs/current/api/org/apache/hadoop/fs/FileSystem.html#delete-org.apache.hadoop.fs.Path-boolean-}
azureDataLakeDelete <- function(azureActiveContext, azureDataLakeAccount, relativePath, recursive = FALSE, verbose = FALSE) {
  if (!missing(azureActiveContext) && !is.null(azureActiveContext)) {
    assert_that(is.azureActiveContext(azureActiveContext))
    azureCheckToken(azureActiveContext)
  }
  assert_that(is_storage_account(azureDataLakeAccount))
  assert_that(is_relativePath(relativePath))
  URL <- paste0(
    getAzureDataLakeBasePath(azureDataLakeAccount),
    getAzureDataLakeURLEncodedString(relativePath),
    "?op=DELETE",
    getAzureDataLakeApiVersion()
  )
  if (!missing(recursive)  && !is.null(recursive)) URL <- paste0(URL, "&recursive=", recursive)
  resHttp <- callAzureDataLakeApi(URL, verb = "DELETE",
                                  azureActiveContext = azureActiveContext,
                                  verbose = verbose)
  stopWithAzureError(resHttp)
  resJsonStr <- content(resHttp, "text", encoding = "UTF-8")
  resJsonObj <- jsonlite::fromJSON(resJsonStr)
  resDf <- as.data.frame(resJsonObj)
  return(resDf$boolean)
}




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
createAdlFileOutputStream <- function(azureActiveContext, accountName, relativePath, verbose = FALSE) {
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
  res <- azureDataLakeGetFileStatus(azureActiveContext, accountName, relativePath, verbose)
  azEnv$remoteCursor <- as.integer(res$FileStatus.length) # this remote cursor starts from 0
  azEnv$streamClosed <- FALSE
  azEnv$lastFlushUpdatedMetadata <- FALSE

  return(azEnv)
}

#' Write to an adlFileOutputStream.
#'
#' @param adlFileOutputStream adlFileOutputStream of the file
#' @param contents contents to write to the file
#' @param off the start offset in the data (must be > 1)
#' @param len the number of bytes to write
#' @param verbose Print tracing information (default FALSE)
#' @return NULL (void)
#'
#' @family Azure Data Lake Store functions
#' @export
adlFileOutputStreamWrite <- function(adlFileOutputStream, contents, off, len, 
                                     verbose = FALSE) {
  if (!missing(adlFileOutputStream) && !is.null(adlFileOutputStream)) {
    assert_that(is.adlFileOutputStream(adlFileOutputStream))
    adlFileOutputStreamCheck(adlFileOutputStream)
  }
  if (!is.null(adlFileOutputStream$azureActiveContext)) {
    assert_that(is.azureActiveContext(adlFileOutputStream$azureActiveContext))
    azureCheckToken(adlFileOutputStream$azureActiveContext)
  }
  assert_that(is_content(contents))
  contentlen <- getContentSize(contents)
  if (off < 1 || off > contentlen || len < 0 
      || (off + len) > contentlen + 1
      || (off + len) < 1) {
    stop("IndexOutOfBoundsException: specify valid offset and length")
  }
  if (off > contentlen + 1 || len > (contentlen + 1 - off)) {
    stop("IllegalArgumentException: array offset and length are > array size")
  }
  if (len == 0) {
    return(NULL)
  }

  # TODO: need to implement a buffer manager ?
  adlFileOutputStream$buffer <- raw(adlFileOutputStream$blockSize)

  # if len > 4MB, then we force-break the write into 4MB chunks
  while (len > adlFileOutputStream$blockSize) {
    adlFileOutputStreamFlush(adlFileOutputStream, syncFlagEnum$DATA, verbose) # flush first, because we want to preserve record boundary of last append
    addToBuffer(adlFileOutputStream, contents, off, adlFileOutputStream$blockSize)
    off <- off + adlFileOutputStream$blockSize
    len <- len - adlFileOutputStream$blockSize
  }
  # now len == the remaining length

  # if adding this to buffer would overflow buffer, then flush buffer first
  if (len > getContentSize(adlFileOutputStream$buffer) - (adlFileOutputStream$cursor - 1)) {
    adlFileOutputStreamFlush(adlFileOutputStream, syncFlagEnum$DATA, verbose)
  }
  # now we know b will fit in remaining buffer, so just add it in
  addToBuffer(adlFileOutputStream, contents, off, len)

  return(NULL)
}

addToBuffer <- function(adlFileOutputStream, contents, off, len) {
  bufferlen <- getContentSize(adlFileOutputStream$buffer)
  cursor <- adlFileOutputStream$cursor
  if (len > bufferlen - (cursor - 1)) { # if requesting to copy more than remaining space in buffer
    stop("IllegalArgumentException: invalid buffer copy requested in addToBuffer")
  }
  # optimized arraycopy
  adlFileOutputStream$buffer[cursor : (cursor + len - 1)] <- contents[off : (off + len - 1)]
  adlFileOutputStream$cursor <- as.integer(cursor + len)
}

#' Flush an adlFileOutputStream.
#'
#' @param adlFileOutputStream adlFileOutputStream of the file
#' @param syncFlag type of sync (DATA, METADATA, CLOSE)
#' @param verbose Print tracing information (default FALSE)
#' @return NULL (void)
#'
#' @family Azure Data Lake Store functions
#' @export
adlFileOutputStreamFlush <- function(adlFileOutputStream, syncFlag = syncFlagEnum$METADATA,
                                     verbose = FALSE) {
  # Ignoring this, because HBase actually calls flush after close() <sigh>
  if (adlFileOutputStream$streamClosed) {
    return(NULL)
  }
  # nothing to flush
  if (adlFileOutputStream$cursor == 1 
      && (syncFlag == syncFlagEnum$DATA || syncFlag == syncFlagEnum$PIPELINE)) {
    return(NULL)
  }
  # do not send a flush if the last flush updated metadata and there is no data
  if ((adlFileOutputStream$cursor == 1)
      && adlFileOutputStream$lastFlushUpdateMetadata
      && (syncFlag == syncFlagEnum$METADATA)) {
    return(NULL)
  }
  resHttp <- azureDataLakeAppendCore(adlFileOutputStream$azureActiveContext, 
                             adlFileOutputStream$accountName, adlFileOutputStream$relativePath, 4194304L, 
                             adlFileOutputStream$buffer, as.integer(adlFileOutputStream$cursor - 1), 
                             adlFileOutputStream$leaseId, adlFileOutputStream$leaseId, syncFlag, 
                             adlFileOutputStream$remoteCursor,
                             verbose)
  # TODO: implement - retry/error recovery (?)
  stopWithAzureError(resHttp)
  adlFileOutputStream$remoteCursor <- (adlFileOutputStream$remoteCursor + (adlFileOutputStream$cursor - 1))
  adlFileOutputStream$cursor <- 1L
  adlFileOutputStream$lastFlushUpdatedMetadata <- (syncFlag != syncFlagEnum$DATA)
  return(NULL)
}

#' Close an adlFileOutputStream.
#'
#' @param adlFileOutputStream adlFileOutputStream of the file
#' @param verbose Print tracing information (default FALSE)
#' @return NULL (void)
#'
#' @family Azure Data Lake Store functions
#' @export
adlFileOutputStreamClose <- function(adlFileOutputStream,
                                     verbose = FALSE) {
  if(adlFileOutputStream$streamClosed) return(NULL) # Return silently upon multiple closes
  adlFileOutputStreamFlush(adlFileOutputStream, syncFlagEnum$CLOSE, verbose)
  adlFileOutputStream$streamClosed <- TRUE
  adlFileOutputStream$buffer <- raw(0) # release byte buffer so it can be GC'ed even if app continues to hold reference to stream
  return(NULL)
}


#' Create an createAdlFileInputStream
#' Create a container (`adlFileInputStream`) for holding variables used by the Azure Data Lake Store data functions.
#'
#' @inheritParams setAzureContext
#' @param accountName the account name
#' @param relativePath Relative path of a file/directory
#' @param verbose Print tracing information (default FALSE).
#' @return An `adlFileOutputStream` object
#'
#' @family Azure Data Lake Store functions
createAdlFileInputStream <- function(azureActiveContext, accountName, relativePath, verbose = FALSE) {
  azEnv <- new.env(parent = emptyenv())
  azEnv <- as.adlFileInputStream(azEnv)
  list2env(
    list(azureActiveContext = "", accountName = "", relativePath = ""),
    envir = azEnv
  )
  if (!missing(azureActiveContext)) azEnv$azureActiveContext <- azureActiveContext
  if (!missing(accountName)) azEnv$accountName <- accountName
  if (!missing(relativePath)) azEnv$relativePath <- relativePath
  azEnv$directoryEntry <- azureDataLakeGetFileStatus(azureActiveContext, accountName, relativePath, verbose)
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

#' Read an adlFileInputStream.
#'
#' @param adlFileInputStream adlFileInputStream of the file
#' @param position position in file to read from (starts from zero)
#' @param buffer raw buffer to read into
#' @param offset offset into the byte buffer at which to read the data into
#' @param length number of bytes to read
#' @param verbose Print tracing information (default FALSE)
#' @return list that contains number of bytes read and the `buffer`
#'
#' @family Azure Data Lake Store functions
#' @export
adlFileInputStreamRead <- function(adlFileInputStream, 
                                   position, buffer, offset, length, 
                                   verbose = FALSE) {
  if (!missing(adlFileInputStream) && !is.null(adlFileInputStream)) {
    assert_that(is.adlFileInputStream(adlFileInputStream))
    adlFileInputStreamCheck(adlFileInputStream)
  }
  assert_that(is_position(position))
  assert_that(is_content(buffer))
  assert_that(is_offset(offset))
  assert_that(is_length(length))

  if (position < 0) {
    stop("IllegalArgumentException: attempting to read from negative offset")
  }
  if (position >= adlFileInputStream$directoryEntry$FileStatus.length) {
    return -1;  # Hadoop prefers -1 to EOFException
  }
  if (is.null(buffer)) {
    stop("IllegalArgumentException: null byte array passed in to read() method")
  }
  if (offset < 1) {
    stop("IllegalArgumentException: offset less than 1")
  }
  if (offset >= getContentSize(buffer)) {
    stop("IllegalArgumentException: offset greater than length of array")
  }
  if (length < 0) {
    stop("IllegalArgumentException: requested read length is less than zero")
  }
  if (length > (getContentSize(buffer) - (offset - 1))) {
    stop("IllegalArgumentException: requested read length is more than will fit after requested offset in buffer")
  }

  resHttp <- azureDataLakeReadCore(adlFileInputStream$azureActiveContext, 
                    adlFileInputStream$accountName, adlFileInputStream$relativePath, 
                    position, length, 
                    verbose = verbose)
  stopWithAzureError(resHttp)
  resRaw <- content(resHttp, "raw", encoding = "UTF-8")
  resRawLen <- getContentSize(resRaw)
  buffer[offset:(offset + length - 1)] <- resRaw[1:resRawLen]
  res <- list(resRawLen, buffer)
  return(res)
}

#' Buffered read an adlFileInputStream.
#'
#' @param adlFileInputStream adlFileInputStream of the file
#' @param buffer raw buffer to read into
#' @param offset offset into the byte buffer at which to read the data into
#' @param length number of bytes to read
#' @param verbose Print tracing information (default FALSE)
#' @return list that contains number of bytes read and the `buffer`
#'
#' @family Azure Data Lake Store functions
#' @export
adlFileInputStreamReadBuffered <- function(adlFileInputStream, 
                                   buffer, offset, length, 
                                   verbose = FALSE) {
  if (!missing(adlFileInputStream) && !is.null(adlFileInputStream)) {
    assert_that(is.adlFileInputStream(adlFileInputStream))
    adlFileInputStreamCheck(adlFileInputStream)
  }
  if (!is.null(adlFileInputStream$azureActiveContext)) {
    assert_that(is.azureActiveContext(adlFileInputStream$azureActiveContext))
    azureCheckToken(adlFileInputStream$azureActiveContext)
  }
  assert_that(is_content(buffer))
  assert_that(is_offset(offset))
  assert_that(is_length(length))

  if (offset < 1L || length < 0L || length > (getContentSize(buffer) - (offset - 1L))) {
    stop("IndexOutOfBoundsException")
  }
  if (length == 0) {
    res <- list(0, buffer)
    return(res)
  }

  #If buffer is empty, then fill the buffer. If EOF, then return -1
  if (adlFileInputStream$bCursor == adlFileInputStream$limit) {
    if (readFromService(adlFileInputStream) < 0) {
      res <- list(-1, buffer)
      return(res)
    }
  }

  bytesRemaining <- (adlFileInputStream$limit - adlFileInputStream$bCursor)
  limits <- c(length, bytesRemaining)
  bytesToRead <- limits[which.min(limits)]
  buffer[offset:(offset - 1 + bytesToRead)] <- 
    adlFileInputStream$buffer[adlFileInputStream$bCursor:(adlFileInputStream$bCursor - 1 + bytesToRead)]
  adlFileInputStream$bCursor <- adlFileInputStream$bCursor + bytesToRead
  res <- list(bytesToRead, buffer)
  return(res)
}

#' Read from service attempts to read `blocksize` bytes from service.
#' Returns how many bytes are actually read, could be less than blocksize.
#'
#' @param adlFileInputStream the `adlFileInputStream` object to read from
#' @param verbose Print tracing information (default FALSE)
#' @return number of bytes actually read
#' 
#' @family Azure Data Lake Store functions
readFromService <- function(adlFileInputStream, verbose = FALSE) {
  if (adlFileInputStream$bCursor < adlFileInputStream$limit) return(0) #if there's still unread data in the buffer then dont overwrite it At or past end of file
  if (adlFileInputStream$fCursor >= adlFileInputStream$directoryEntry$FileStatus.length) return(-1)
  if (adlFileInputStream$directoryEntry$FileStatus.length <= adlFileInputStream$blockSize)
    return(slurpFullFile(adlFileInputStream))

  #reset buffer to initial state - i.e., throw away existing data
  adlFileInputStream$bCursor <- 1L
  adlFileInputStream$limit <- 1L
  if (is.null(adlFileInputStream$buffer)) adlFileInputStream$buffer <- raw(getAzureDataLakeDefaultBufferSize())

  resHttp <- azureDataLakeReadCore(adlFileInputStream$azureActiveContext, 
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
slurpFullFile <- function(adlFileInputStream, verbose = FALSE) {
  if (is.null(adlFileInputStream$buffer)) {
    adlFileInputStream$blocksize <- adlFileInputStream$directoryEntry$FileStatus.length
    adlFileInputStream$buffer <- raw(adlFileInputStream$directoryEntry$FileStatus.length)
  }

  #reset buffer to initial state - i.e., throw away existing data
  adlFileInputStream$bCursor <- adlFileInputStreamGetPos(adlFileInputStream) + 1L  # preserve current file offset (may not be 0 if app did a seek before first read)
  adlFileInputStream$limit <- 1L
  adlFileInputStream$fCursor <- 0L  # read from beginning

  resHttp <- azureDataLakeReadCore(adlFileInputStream$azureActiveContext, 
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

#' Seek to given position in stream.
#'
#' @param adlFileInputStream adlFileInputStream of the file
#' @param n position to seek to
#' @return NULL (void)
#' Exception IOException if there is an error
#' Exception EOFException if attempting to seek past end of file
#'
#' @family Azure Data Lake Store functions
#' @export
adlFileInputStreamSeek <- function(adlFileInputStream, n) {
  if (!missing(adlFileInputStream) && !is.null(adlFileInputStream)) {
    assert_that(is.adlFileInputStream(adlFileInputStream))
    adlFileInputStreamCheck(adlFileInputStream)
  }

  if (adlFileInputStream$streamClosed) stop("IOException: attempting to seek into a closed stream")
  if (n<0) stop("EOFException: Cannot seek to before the beginning of file")
  if (n>adlFileInputStream$directoryEntry$FileStatus.length) stop("EOFExceptionCannot: seek past end of file")

  if (n>=adlFileInputStream$fCursor-adlFileInputStream$limit && n<=adlFileInputStream$fCursor) { # within buffer
    adlFileInputStream$bCursor <- (n-(adlFileInputStream$fCursor-adlFileInputStream$limit));
    return;
  }

  # next read will read from here
  adlFileInputStream$fCursor = n

  #invalidate buffer
  adlFileInputStream$limit = 0;
  adlFileInputStream$bCursor = 0;
  
  return(NULL)
}

#' Skip to given position in stream.
#'
#' @param adlFileInputStream adlFileInputStream of the file
#' @param n position to seek to
#' @return Skips over and discards n bytes of data from the input stream. 
#'     The skip method may, for a variety of reasons, end up skipping over some smaller 
#'     number of bytes, possibly 0. The actual number of bytes skipped is returned.
#' Exception IOException if there is an error
#' Exception EOFException if attempting to seek past end of file
#'
#' @family Azure Data Lake Store functions
#' @export
adlFileInputStreamSkip <- function(adlFileInputStream, n) {
  if (!missing(adlFileInputStream) && !is.null(adlFileInputStream)) {
    assert_that(is.adlFileInputStream(adlFileInputStream))
    adlFileInputStreamCheck(adlFileInputStream)
  }

  if (adlFileInputStream$streamClosed) stop("IOException: attempting to seek into a closed stream")
  currentPos <- adlFileInputStreamGetPos(adlFileInputStream)
  newPos <- (currentPos + n)
  if (newPos < 0) {
    newPos <- 0
    n <- (newPos - currentPos)
  }
  if (newPos > adlFileInputStream$directoryEntry$FileStatus.length) {
    newPos <- adlFileInputStream$directoryEntry$FileStatus.length
    n <- newPos - currentPos
  }
  adlFileInputStreamSeek(adlFileInputStream, newPos)
  return(n)
}

#' returns the remaining number of bytes available to read from the buffer, without having to call
#' the server
#'
#' @param adlFileInputStream adlFileInputStream of the file
#' @return the number of bytes availabel
#' Exception IOException throws `ADLException` if call fails
#'
#' @family Azure Data Lake Store functions
#' @export
adlFileInputStreamAvailable <- function(adlFileInputStream) {
  if (!missing(adlFileInputStream) && !is.null(adlFileInputStream)) {
    assert_that(is.adlFileInputStream(adlFileInputStream))
    adlFileInputStreamCheck(adlFileInputStream)
  }

  if (adlFileInputStream$streamClosed) stop("IOException: attempting to call available() on a closed stream")
  return(adlFileInputStream$limit - adlFileInputStream$bCursor)
}

#' Returns the length of the file that this stream refers to. Note that the length returned is the length
#' as of the time the Stream was opened. Specifically, if there have been subsequent appends to the file,
#' they wont be reflected in the returned length.
#'
#' @param adlFileInputStream adlFileInputStream of the file
#' @return length of the file.
#' Exception IOException if the stream is closed
#' 
#' @family Azure Data Lake Store functions
#' @export
adlFileInputStreamLength <- function(adlFileInputStream) {
  if (!missing(adlFileInputStream) && !is.null(adlFileInputStream)) {
    assert_that(is.adlFileInputStream(adlFileInputStream))
    adlFileInputStreamCheck(adlFileInputStream)
  }

  if (adlFileInputStream$streamClosed) stop("IOException: attempting to call length() on a closed stream")
  return(adlFileInputStream$directoryEntry$FileStatus.length)
}

#' gets the position of the cursor within the file
#'
#' @param adlFileInputStream adlFileInputStream of the file
#' @return position of the cursor
#' Exception IOException
#'
#' @family Azure Data Lake Store functions
#' @export
adlFileInputStreamGetPos <- function(adlFileInputStream) {
  if (!missing(adlFileInputStream) && !is.null(adlFileInputStream)) {
    assert_that(is.adlFileInputStream(adlFileInputStream))
    adlFileInputStreamCheck(adlFileInputStream)
  }

  if(adlFileInputStream$streamClosed) {
    stop("IOException: attempting to call getPos() on a closed stream")
  }
  return(adlFileInputStream$fCursor - adlFileInputStream$limit + adlFileInputStream$bCursor)
}

#' Close an adlFileInputStream.
#'
#' @param adlFileInputStream adlFileInputStream of the file
#' @param verbose Print tracing information (default FALSE)
#' @return NULL (void)
#'
#' @family Azure Data Lake Store functions
#' @export
adlFileInputStreamClose <- function(adlFileInputStream,
                                     verbose = FALSE) {
  if (!missing(adlFileInputStream) && !is.null(adlFileInputStream)) {
    assert_that(is.adlFileInputStream(adlFileInputStream))
    adlFileInputStreamCheck(adlFileInputStream)
  }

  if(adlFileInputStream$streamClosed) return(NULL) # Return silently upon multiple closes
  adlFileInputStream$streamClosed <- TRUE
  adlFileInputStream$buffer <- raw(0) # release byte buffer so it can be GC'ed even if app continues to hold reference to stream
  return(NULL)
}

#' Not supported by this stream. Throws `UnsupportedOperationException`
#'
#' @param adlFileInputStream adlFileInputStream of the file
#' @param readLimit ignored
#' @return NULL (void)
#'
#' @family Azure Data Lake Store functions
#' @export
adlFileInputStreamMark <- function(adlFileInputStream, readLimit) {
  if (!missing(adlFileInputStream) && !is.null(adlFileInputStream)) {
    assert_that(is.adlFileInputStream(adlFileInputStream))
    adlFileInputStreamCheck(adlFileInputStream)
  }

  stop(paste0("UnsupportedOperationException: mark()/reset() not supported on this stream - readLimit: ", readLimit))
}

#' Not supported by this stream. Throws `UnsupportedOperationException`
#'
#' @param adlFileInputStream adlFileInputStream of the file
#' @return NULL (void)
#'
#' @family Azure Data Lake Store functions
#' @export
adlFileInputStreamReset <- function(adlFileInputStream) {
  if (!missing(adlFileInputStream) && !is.null(adlFileInputStream)) {
    assert_that(is.adlFileInputStream(adlFileInputStream))
    adlFileInputStreamCheck(adlFileInputStream)
  }

  stop("UnsupportedOperationException: mark()/reset() not supported on this stream")
}

#' gets whether mark and reset are supported by `ADLFileInputStream`. Always returns false.
#'
#' @param adlFileInputStream adlFileInputStream of the file
#' @return FALSE (always)
#'
#' @family Azure Data Lake Store functions
#' @export
adlFileInputStreamMarkSupported <- function(adlFileInputStream) {
  if (!missing(adlFileInputStream) && !is.null(adlFileInputStream)) {
    assert_that(is.adlFileInputStream(adlFileInputStream))
    adlFileInputStreamCheck(adlFileInputStream)
  }

  return(FALSE)
}
