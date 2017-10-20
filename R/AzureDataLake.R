#' List the statuses of the files/directories in the given path.
#'
#' @inheritParams setAzureContext
#' @param azureDataLakeAccount Name of the Azure Data Lake Store account.
#' @param relativePath Relative path of a file/directory.
#' @param verbose Print tracing information (default FALSE).
#' @return Returns a FileStatuses data frame with a row for each directory element. 
#'         Each row has 11 columns (12 in case of a file):
#'         FileStatuses.FileStatus.(accessTime, modificationTime, replication, permission, owner, group, aclBit, msExpirationtime (in case of file))
#' @exception FileNotFoundException
#' @exception IOException
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
    relativePath,
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
  resDf
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
#' @exception FileNotFoundException
#' @exception IOException
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
    relativePath,
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
#' @exception IOException
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
    relativePath,
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
#' @exception IOException
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
    relativePath,
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
                                  content = contents, contenttype = "text/plain; charset=UTF-8",
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
#' @param verbose Print tracing information (default FALSE).
#' @return NULL (void)
#' @exception IOException
#' 
#' @family Azure Data Lake Store functions
#' @export
#' 
#' @references \url{https://docs.microsoft.com/en-us/azure/data-lake-store/data-lake-store-data-operations-rest-api#upload-data}
#' @seealso \url{https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Append_to_a_File}
#' @seealso \url{https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Buffer_Size}
#' @seealso \url{https://hadoop.apache.org/docs/current/api/org/apache/hadoop/fs/FileSystem.html#append-org.apache.hadoop.fs.Path-int-org.apache.hadoop.util.Progressable-}
azureDataLakeAppend <- function(azureActiveContext, azureDataLakeAccount, relativePath, 
                                bufferSize, contents, verbose = FALSE) {
  if (!missing(azureActiveContext) && !is.null(azureActiveContext)) {
    assert_that(is.azureActiveContext(azureActiveContext))
    azureCheckToken(azureActiveContext)
  }
  assert_that(is_storage_account(azureDataLakeAccount))
  assert_that(is_relativePath(relativePath))
  if (!missing(bufferSize) && !is.null(bufferSize)) assert_that(is_bufferSize(bufferSize))
  assert_that(is_content(contents))
  # avoid a zero byte append
  if (getContentSize(contents) == 0) {
    return(NULL)
  }
  URL <- paste0(
    getAzureDataLakeBasePath(azureDataLakeAccount),
    relativePath,
    "?op=APPEND", "&append=true",
    getAzureDataLakeApiVersion()
  )
  if (!missing(bufferSize) && !is.null(bufferSize)) URL <- paste0(URL, "&buffersize=", bufferSize)
  resHttp <- callAzureDataLakeApi(URL, verb = "POST",
                                  azureActiveContext = azureActiveContext,
                                  content = contents, contenttype = "text/plain; charset=UTF-8",
                                  verbose = verbose)
  stopWithAzureError(resHttp)
  # retrun a NULL (void)
  return(NULL)
}

#' Open and read a file.
#'
#' @inheritParams setAzureContext
#' @param azureDataLakeAccount Name of the Azure Data Lake account.
#' @param relativePath Relative path of a file/directory.
#' @param offset Provide the offset to read from.
#' @param length Provide length of data to read.
#' @param bufferSize Size of the buffer to be used.
#' @param verbose Print tracing information (default FALSE).
#' @return raw contents of the file.
#' @exception IOException
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
azureDataLakeRead <- function(azureActiveContext, azureDataLakeAccount, relativePath, offset, length, bufferSize, verbose = FALSE) {
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
    relativePath,
    "?op=OPEN", "&read=true",
    getAzureDataLakeApiVersion()
  )
  if (!missing(offset) && !is.null(offset)) URL <- paste0(URL, "&offset=", offset)
  if (!missing(length) && !is.null(length)) URL <- paste0(URL, "&length=", length)
  if (!missing(bufferSize) && !is.null(bufferSize)) URL <- paste0(URL, "&buffersize=", bufferSize)
  resHttp <- callAzureDataLakeApi(URL,
                                  azureActiveContext = azureActiveContext,
                                  verbose = verbose)
  stopWithAzureError(resHttp)
  resRaw <- content(resHttp, "raw", encoding = "UTF-8")
  return(resRaw)
}

#' Delete a file/directory.
#'
#' @inheritParams setAzureContext
#' @param azureDataLakeAccount Name of the Azure Data Lake account.
#' @param relativePath Relative path of a file/directory.
#' @param recursive If path is a directory, recursively delete contents and directory (default FALSE).
#' @param verbose Print tracing information (default FALSE).
#' @return true if delete is successful else false.
#' @exception IOException
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
    relativePath,
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
