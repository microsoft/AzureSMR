
#' List status for specified relative path of an azure data lake account.
#'
#' @inheritParams createAzureContext
#' @inheritParams azureAuthenticate
#'
#' @param azureActiveContext Provide an `azureActiveContext` object used for authentication.
#' @param azureDataLakeAccount Provide the name of the Azure Data Lake account.
#' @param relativePath Provide a relative path of the directory.
#' @param verbose Print tracing information (default FALSE).
#'
#' @return Returns a data frame.
#'
#' @family Azure Data Lake Store functions
#' @export
azureDataLakeListStatus <- function(azureActiveContext, azureDataLakeAccount, relativePath = "", verbose = FALSE) {

  if (!missing(azureActiveContext) && !is.null(azureActiveContext)) {
    assert_that(is.azureActiveContext(azureActiveContext))
    azureCheckToken(azureActiveContext)
  }
  assert_that(is_storage_account(azureDataLakeAccount))
  verbosity <- set_verbosity(verbose)

  URL <- paste0(
    "https://", azureDataLakeAccount, ".azuredatalakestore.net/webhdfs/v1/",
    relativePath,
    "?op=LISTSTATUS",
    "&api-version=2016-11-01"
    )

  resHttp <- callAzureDataLakeApi(URL,
    azureActiveContext = azureActiveContext,
    verbose = verbose)

  stopWithAzureError(resHttp)

  resJsonStr <- content(resHttp, "text", encoding = "UTF-8")
  resJsonObj <- jsonlite::fromJSON(resJsonStr)
  if (length(resJsonObj$FileStatuses$FileStatus) == 0) {
    #return empty data frame in case of an empty json object
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
        FileStatuses.FileStatus.aclBit = character(0)
      )
    )
  }
  resDf <- as.data.frame(resJsonObj)
  resDf
}


#' Azure Data Lake GETFILESTATUS for specified relativePath of an azure data lake account.
#'
#' @inheritParams createAzureContext
#' @inheritParams azureAuthenticate
#'
#' @param azureActiveContext Provide an `azureActiveContext` object used for authentication.
#' @param azureDataLakeAccount Provide the name of the Azure Data Lake account.
#' @param relativePath Provide a relative path of the directory.
#' @param verbose Print tracing information (default FALSE).
#'
#' @return Returns a data frame.
#'
#' @family Azure Data Lake Store functions
#' @export
azureDataLakeGetFileStatus <- function(azureActiveContext, azureDataLakeAccount, relativePath = "", verbose = FALSE) {
  
  if (!missing(azureActiveContext) && !is.null(azureActiveContext)) {
    assert_that(is.azureActiveContext(azureActiveContext))
    azureCheckToken(azureActiveContext)
  }
  assert_that(is_storage_account(azureDataLakeAccount))
  verbosity <- set_verbosity(verbose)
  
  URL <- paste0(
    "https://", azureDataLakeAccount, ".azuredatalakestore.net/webhdfs/v1/",
    relativePath,
    "?op=GETFILESTATUS",
    "&api-version=2016-11-01"
  )
  
  resHttp <- callAzureDataLakeApi(URL,
                                  azureActiveContext = azureActiveContext,
                                  verbose = verbose)
  
  if (status_code(resHttp) == 404) {
    warning("Azure data lake response: resource not found")
    return(NULL)
  }
  stopWithAzureError(resHttp)
  
  resJsonStr <- content(resHttp, "text", encoding = "UTF-8")
  resJsonObj <- jsonlite::fromJSON(resJsonStr)
  if (length(resJsonObj$FileStatus) == 0) {
    #return empty data frame in case of an empty json object
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
        FileStatuses.FileStatus.aclBit = character(0)
      )
    )
  }
  resDf <- as.data.frame(resJsonObj)
  resDf
}


#' Azure Data Lake MKDIRS for specified relativePath of an azure data lake account.
#'
#' @inheritParams createAzureContext
#' @inheritParams azureAuthenticate
#'
#' @param azureActiveContext Provide an `azureActiveContext` object used for authentication.
#' @param azureDataLakeAccount Provide the name of the Azure Data Lake account.
#' @param relativePath Provide a relative path of the directory.
#' @param permission Provide the permission to be set for the directory.
#' @param verbose Print tracing information (default FALSE).
#'
#' @return Returns a boolean.
#'
#' @family Azure Data Lake Store functions
#' @export
azureDataLakeMkdirs <- function(azureActiveContext, azureDataLakeAccount, relativePath, permission = NULL, verbose = FALSE) {

  if (!missing(azureActiveContext) && !is.null(azureActiveContext)) {
    assert_that(is.azureActiveContext(azureActiveContext))
    azureCheckToken(azureActiveContext)
  }
  assert_that(is_storage_account(azureDataLakeAccount))
  if (!missing(permission) && !is.null(permission)) assert_that(is_permission(permission))
  verbosity <- set_verbosity(verbose)

  URL <- paste0(
    "https://", azureDataLakeAccount, ".azuredatalakestore.net/webhdfs/v1/",
    relativePath,
    "?op=MKDIRS",
    "&api-version=2016-11-01"
  )
  if (!missing(permission) && !is.null(permission)) URL <- paste0(URL, "&permission=", permission)

  resHttp <- callAzureDataLakeApi(URL, verb = "PUT",
                                  azureActiveContext = azureActiveContext,
                                  verbose = verbose)
  if (status_code(resHttp) == 404) {
    warning("Azure data lake response: resource not found")
    return(NULL)
  }
  stopWithAzureError(resHttp)

  resJsonStr <- content(resHttp, "text", encoding = "UTF-8")
  resJsonObj <- jsonlite::fromJSON(resJsonStr)
  resDf <- as.data.frame(resJsonObj)
  resDf$boolean
}

#' Azure Data Lake CREATE for specified relativePath of an azure data lake account.
#'
#' @inheritParams createAzureContext
#' @inheritParams azureAuthenticate
#'
#' @param azureActiveContext Provide an `azureActiveContext` object used for authentication.
#' @param azureDataLakeAccount Provide the name of the Azure Data Lake account.
#' @param relativePath Provide a relative path of the directory.
#' @param overwrite Overwrite existing files (default FALSE).
#' @param permission Provide the permission to be set for the directory.
#' @param contents Provide contents to write to `relativePath`
#' @param verbose Print tracing information (default FALSE).
#'
#' @return NULL
#'
#' @family Azure Data Lake Store functions
#' @export
azureDataLakeCreate <- function(azureActiveContext, azureDataLakeAccount, relativePath, overwrite = FALSE, permission = NULL, contents = "", verbose = FALSE) {
  
  if (!missing(azureActiveContext) && !is.null(azureActiveContext)) {
    assert_that(is.azureActiveContext(azureActiveContext))
    azureCheckToken(azureActiveContext)
  }
  assert_that(is_storage_account(azureDataLakeAccount))
  if (!missing(permission) && !is.null(permission)) assert_that(is_permission(permission))
  # TODO: Need a check for contents ?
  #assert_that(is_content(contents))
  verbosity <- set_verbosity(verbose)
  
  URL <- paste0(
    "https://", azureDataLakeAccount, ".azuredatalakestore.net/webhdfs/v1/",
    relativePath,
    "?op=CREATE&write=true",
    "&api-version=2016-11-01"
  )
  if (!missing(overwrite)  && !is.null(overwrite)) URL <- paste0(URL, "&overwrite=", overwrite)
  if (!missing(permission)  && !is.null(permission)) URL <- paste0(URL, "&permission=", permission)
  
  resHttp <- callAzureDataLakeApi(URL, verb = "PUT",
                                  azureActiveContext = azureActiveContext,
                                  content = contents, contenttype = "text/plain; charset=UTF-8",
                                  verbose = verbose)
  stopWithAzureError(resHttp)
  # TODO: Check why this returns NULL
}

#' Azure Data Lake APPEND for specified relativePath of an azure data lake account.
#'
#' @inheritParams createAzureContext
#' @inheritParams azureAuthenticate
#'
#' @param azureActiveContext Provide an `azureActiveContext` object used for authentication.
#' @param azureDataLakeAccount Provide the name of the Azure Data Lake account.
#' @param relativePath Provide a relative path of the directory.
#' @param contents Provide contents to write to `relativePath`
#' @param verbose Print tracing information (default FALSE).
#'
#' @return NULL
#'
#' @family Azure Data Lake Store functions
#' @export
azureDataLakeAppend <- function(azureActiveContext, azureDataLakeAccount, relativePath, contents = "", verbose = FALSE) {
  
  if (!missing(azureActiveContext) && !is.null(azureActiveContext)) {
    assert_that(is.azureActiveContext(azureActiveContext))
    azureCheckToken(azureActiveContext)
  }
  assert_that(is_storage_account(azureDataLakeAccount))
  # TODO: Need a check for contents ?
  #assert_that(is_content(contents))
  verbosity <- set_verbosity(verbose)
  
  URL <- paste0(
    "https://", azureDataLakeAccount, ".azuredatalakestore.net/webhdfs/v1/",
    relativePath,
    "?op=APPEND&append=true",
    "&api-version=2016-11-01"
  )
  
  resHttp <- callAzureDataLakeApi(URL, verb = "POST",
                                  azureActiveContext = azureActiveContext,
                                  content = contents, contenttype = "text/plain; charset=UTF-8",
                                  verbose = verbose)
  stopWithAzureError(resHttp)
  # TODO: Check why this returns NULL
}

#' Azure Data Lake OPEN for specified relativePath of an azure data lake account.
#'
#' @inheritParams createAzureContext
#' @inheritParams azureAuthenticate
#'
#' @param azureActiveContext Provide an `azureActiveContext` object used for authentication.
#' @param azureDataLakeAccount Provide the name of the Azure Data Lake account.
#' @param relativePath Provide a relative path of the directory.
#' @param offset Provide the offset to read from.
#' @param length Provide length of data to read.
#' @param verbose Print tracing information (default FALSE).
#'
#' @return Returns a data frame.
#'
#' @family Azure Data Lake Store functions
#' @export
azureDataLakeOpen <- function(azureActiveContext, azureDataLakeAccount, relativePath, offset, length, verbose = FALSE) {

  if (!missing(azureActiveContext) && !is.null(azureActiveContext)) {
    assert_that(is.azureActiveContext(azureActiveContext))
    azureCheckToken(azureActiveContext)
  }
  assert_that(is_storage_account(azureDataLakeAccount))
  verbosity <- set_verbosity(verbose)

  URL <- paste0(
    "https://", azureDataLakeAccount, ".azuredatalakestore.net/webhdfs/v1/",
    relativePath,
    "?op=OPEN&read=true",
    "&api-version=2016-11-01"
  )
  if (!missing(offset)  && !is.null(offset)) URL <- paste0(URL, "&offset=", offset)
  if (!missing(length)  && !is.null(length)) URL <- paste0(URL, "&length=", length)

  resHttp <- callAzureDataLakeApi(URL,
                                  azureActiveContext = azureActiveContext,
                                  verbose = verbose)
  stopWithAzureError(resHttp)
  
  resStr <- content(resHttp, "text", encoding = "UTF-8")
  resStr
}


#' Azure Data Lake DELETE for specified relativePath of an azure data lake account.
#'
#' @inheritParams createAzureContext
#' @inheritParams azureAuthenticate
#'
#' @param azureActiveContext Provide an `azureActiveContext` object used for authentication.
#' @param azureDataLakeAccount Provide the name of the Azure Data Lake account.
#' @param relativePath Provide a relative path of the directory.
#' @param recursive Provide recursive delete option.
#' @param verbose Print tracing information (default FALSE).
#'
#' @return Returns a boolean.
#'
#' @family Azure Data Lake Store functions
#' @export
azureDataLakeDelete <- function(azureActiveContext, azureDataLakeAccount, relativePath, recursive = FALSE, verbose = FALSE) {
  
  if (!missing(azureActiveContext) && !is.null(azureActiveContext)) {
    assert_that(is.azureActiveContext(azureActiveContext))
    azureCheckToken(azureActiveContext)
  }
  assert_that(is_storage_account(azureDataLakeAccount))
  verbosity <- set_verbosity(verbose)
  
  URL <- paste0(
    "https://", azureDataLakeAccount, ".azuredatalakestore.net/webhdfs/v1/",
    relativePath,
    "?op=DELETE",
    "&api-version=2016-11-01"
  )
  if (!missing(recursive)  && !is.null(recursive)) URL <- paste0(URL, "&recursive=", recursive)
  
  resHttp <- callAzureDataLakeApi(URL, verb = "DELETE",
                                  azureActiveContext = azureActiveContext,
                                  verbose = verbose)
  if (status_code(resHttp) == 404) {
    warning("Azure data lake response: resource not found")
    return(NULL)
  }
  stopWithAzureError(resHttp)
  
  resJsonStr <- content(resHttp, "text", encoding = "UTF-8")
  resJsonObj <- jsonlite::fromJSON(resJsonStr)
  resDf <- as.data.frame(resJsonObj)
  resDf$boolean
}


