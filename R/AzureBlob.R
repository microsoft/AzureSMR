
#' List storage blobs for specified storage account.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @inheritParams azureSAGetKey
#'
#' @param maxresults Optional. Specifies the maximum number of blobs to return, including all BlobPrefix elements. If the request does not specify maxresults or specifies a value greater than 5,000, the server will return up to 5,000 items.  Setting `maxresults` to a value less than or equal to zero results in error response code 400 (Bad Request).
#' @param prefix Optional. Filters the results to return only blobs whose names begin with the specified prefix.
#' @param delimiter Optional. When the request includes this parameter, the operation returns a BlobPrefix element in the response body that acts as a placeholder for all blobs whose names begin with the same substring up to the appearance of the delimiter character. The delimiter may be a single character or a string.
#' @param marker Optional. A string value that identifies the portion of the list to be returned with the next list operation. The operation returns a marker value within the response body if the list returned was not complete. The marker value may then be used in a subsequent call to request the next set of list items.  The marker value is opaque to the client.
#'
#' @return Returns a data frame. This data frame has an attribute called `marker` that can be used with the `marker` argument to return the next set of values.
#' @family Blob store functions
#' @export
azureListStorageBlobs <- function(azureActiveContext, storageAccount, storageKey,
                             container, maxresults, prefix, delimiter, marker, verbose = FALSE) {
                       
  if (!missing(azureActiveContext) && !is.null(azureActiveContext)) {
    assert_that(is.azureActiveContext(azureActiveContext))
    azureCheckToken(azureActiveContext)
    if(missing(storageAccount)) storageAccount <- azureActiveContext$storageAccount
    if(missing(storageKey)) storageKey <- azureActiveContext$storageKey
    if(missing(container)) container <- azureActiveContext$container
  }

  assert_that(is_storage_account(storageAccount))
  assert_that(is_container(container))
  assert_that(is_storage_key(storageKey))

  verbosity <- set_verbosity(verbose)

  URL <- paste0("http://", storageAccount, ".blob.core.windows.net/", container, "?restype=container&comp=list")
  if (!missing(maxresults)  && !is.null(maxresults)) URL <- paste0(URL, "&maxresults=", maxresults)
  if (!missing(prefix)      && !is.null(prefix))     URL <- paste0(URL, "&prefix=", prefix)
  if (!missing(delimiter)   && !is.null(delimiter))  URL <- paste0(URL, "&delimiter=", delimiter)
  if (!missing(marker)      && !is.null(marker))     URL <- paste0(URL, "&marker=", marker)

  r <- callAzureStorageApi(URL, 
    storageKey = storageKey, storageAccount = storageAccount, container = container, 
    verbose = verbose)

  if (status_code(r) == 404) {
    warning("Azure response: container not found")
    return(NULL)
  }
  stopWithAzureError(r)
  
  r <- content(r, "text", encoding = "UTF-8")
  y <- htmlParse(r, encoding = "UTF-8")

  namesx <- xpathApply(y, "//blobs//blob//name", xmlValue)

  if (length(namesx) == 0) {
    warning("container is empty")
    return(
      data.frame(
        name            = character(0),
        lastModified    = character(0),
        length          = character(0),
        type            = character(0),
        leaseState      = character(0),
        stringsAsFactors = FALSE,
        check.names = FALSE
      )
    )
  }

  updateAzureActiveContext(azureActiveContext,
    storageAccount = storageAccount,
    storageKey = storageKey
  )

  getXmlBlob <- function(x, property, path = "//blobs//blob//properties/"){
    pth <- paste0(path, property)
    xpathSApply(y, pth, xmlValue)
  }

  z <- data.frame(
    name            = getXmlBlob(y, "name", path = "//blobs//blob//"),
    lastModified    = getXmlBlob(y, "last-modified"),
    length          = getXmlBlob(y, "content-length"),
    type            = getXmlBlob(y, "blobtype"),
    leaseState      = getXmlBlob(y, "leasestate"),
    stringsAsFactors = FALSE,
    check.names = FALSE
  )
  attr(z, "marker") <- xpathSApply(y, "//nextmarker", xmlValue)
  z
}


#' List blob blobs in a storage account directory.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @inheritParams azureSAGetKey

#' @param directory Blob store directory to list for content
#' @param recursive If TRUE, list blob store directories recursively
#'
#' @family Blob store functions
#' @export
azureBlobLS <- function(azureActiveContext, directory, recursive = FALSE,
                        storageAccount, storageKey, container, resourceGroup, verbose = FALSE) {

  if (!missing(azureActiveContext) && !is.null(azureActiveContext)) {
    assert_that(is.azureActiveContext(azureActiveContext))
    azureCheckToken(azureActiveContext)
    if (missing(storageAccount)) storageAccount <- azureActiveContext$storageAccount
    if (missing(storageKey)) storageKey <- azureActiveContext$storageKey
    if (missing(container)) container <- azureActiveContext$container
    if (missing(resourceGroup)) resourceGroup <- azureActiveContext$resourceGroup
    if (missing(directory)) directory <- azureActiveContext$directory
  } else {
    if(missing(directory)) directory <- "/"
    if(missing(container)) container <- ""
  }
  #browser()

  if(is.null(directory) || directory == "") directory <- "/"

  assert_that(is_resource_group(resourceGroup))
  assert_that(is_storage_account(storageAccount))
  assert_that(is_container(container))
  assert_that(is_directory(directory))


  verbosity <- set_verbosity(verbose)

  if (!grepl("^/", directory)) directory <- paste0("/", directory)
  directory <- gsub("//", "/", directory)

  if (!missing(azureActiveContext) && !is.null(azureActiveContext) && missing(storageKey)) {
    storageKey <- refreshStorageKey(azureActiveContext, storageAccount, resourceGroup)
  }

  assert_that(is_storage_key(storageKey))

  blobs <- azureListStorageBlobs(azureActiveContext, container = container, storageAccount = storageAccount, storageKey = storageKey)

  blobs$name <- paste0("/", blobs$name)
  blobs$name <- gsub("//", "/", blobs$name)


  azureActiveContext$directory <- directory
  azureActiveContext$container <- container
  message(paste("Current directory: ", storageAccount, ">", container, ":", directory, "\n"))

  id_indir <- grep(paste0("^", directory), blobs$name)
  blobs <- blobs[id_indir,]

  if (recursive) return(blobs)

  x <- gsub(paste0("^", directory), "", blobs$name)
  x[!grepl("/", x)] <- ""
  x <- gsub("/.*", "", x)
  dirs <- unique(x)
  dirs <- dirs[nchar(dirs) > 0]
  if (length(dirs)) {
    dir_ids <- sapply(dirs,
      function(x) grep(paste0(directory, x, "/"), blobs$name),
      simplify = FALSE
    )
    dir_start <- sapply(dir_ids, "[", 1)
    dir_remove <- do.call(c, sapply(dir_ids, "[", -1))
    blobs$type[dir_start] <- "directory"
    blobs$name[dir_start] <- paste0(directory, names(dir_ids))
    blobs$length[dir_start] <- NA
    blobs$lastModified[dir_start] <- NA
    blobs <- blobs[ - dir_remove,]
  }

  azureActiveContext$storageAccount <- storageAccount
  azureActiveContext$resourceGroup <- resourceGroup
  azureActiveContext$container <- container

  return(blobs)
  
}


#' Get contents from a specifed storage blob.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @inheritParams azureSAGetKey
#' @inheritParams azureBlobLS

#' @param type String, either "text" or "raw". Passed to [httr::content()]
#'
#' @family Blob store functions
#' @export

azureGetBlob <- function(azureActiveContext, blob, directory, type = "text",
                         storageAccount, storageKey, container, resourceGroup, verbose = FALSE) {
  if (!missing(azureActiveContext) && !is.null(azureActiveContext)) {
    assert_that(is.azureActiveContext(azureActiveContext))
    azureCheckToken(azureActiveContext)
    if (missing(storageAccount)) storageAccount <- azureActiveContext$storageAccount
    if (missing(storageKey)) storageKey <- azureActiveContext$storageKey
    if (missing(container)) container <- azureActiveContext$container
    if (missing(resourceGroup)) resourceGroup <- azureActiveContext$resourceGroup
    if (missing(blob)) blob <- azureActiveContext$blob
    if (missing(directory)) directory <- azureActiveContext$directory
    } else {
      if (missing(directory)) directory <- "/"
      if (missing(container)) container <- ""
    }

  assert_that(is_resource_group(resourceGroup))
  assert_that(is_storage_account(storageAccount))
  assert_that(is_container(container))
  assert_that(is_storage_key(storageKey))
  assert_that(is_blob(blob))

  verbosity <- set_verbosity(verbose)


  if (length(directory) < 1)
    directory <- ""  # No previous Dir value
  if (length(container) < 1) {
    directory <- ""  # No previous Dir value
    container <- ""
  } else if (container != container)
    directory <- ""  # Change of container

  if (nchar(directory) > 0)
    directory <- paste0(directory, "/")

  blob <- paste0(directory, blob)
  blob <- gsub("^/", "", blob)
  blob <- gsub("^\\./", "", blob)

  URL <- paste0("http://", storageAccount, ".blob.core.windows.net/", container, "/", blob)
  r <- callAzureStorageApi(URL, 
    storageKey = storageKey, storageAccount = storageAccount, container = container,
    CMD = paste0("/", blob)
    )

  if (status_code(r) == 404) {
    warning("blob not found")
    return(NULL)
  } 
  stopWithAzureError(r)

  updateAzureActiveContext(azureActiveContext,
    storageAccount = storageAccount,
    resourceGroup = resourceGroup,
    storageKey = storageKey,
    container = container,
    blob = blob
  )

  content(r, type, encoding = "UTF-8")
}


#' Write contents to a specifed storage blob.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @inheritParams azureSAGetKey
#' @inheritParams azureBlobLS
#'
#' @param contents - Object or value to store
#' @param file - Local filename to store in Azure blob
#'
#' @family Blob store functions
#' @export
azurePutBlob <- function(azureActiveContext, blob, contents = "", file = "",
                         directory, storageAccount, storageKey,
                         container, resourceGroup, verbose = FALSE) {
  if (!missing(azureActiveContext) && !is.null(azureActiveContext)) {
    assert_that(is.azureActiveContext(azureActiveContext))
    azureCheckToken(azureActiveContext)
    if (missing(storageAccount)) storageAccount <- azureActiveContext$storageAccount
    if (missing(storageKey)) storageKey <- azureActiveContext$storageKey
    if (missing(container)) container <- azureActiveContext$container
    if (missing(resourceGroup)) resourceGroup <- azureActiveContext$resourceGroup
    if (missing(blob)) blob <- azureActiveContext$blob
    if (missing(directory)) directory <- azureActiveContext$directory
    if(missing(container)) container <- azureActiveContext$container
    } else {
      if (missing(directory)) directory <- "/"
      if (missing(container)) container <- ""
    }

  assert_that(is_resource_group(resourceGroup))
  assert_that(is_storage_account(storageAccount))
  assert_that(is_container(container))
  assert_that(is_storage_key(storageKey))
  assert_that(is_blob(blob))

  verbosity <- set_verbosity(verbose)

  if (!grep("/$", directory)) directory <- paste0(directory, "/")

  blob <- paste0(directory, blob)
  blob <- gsub("^/", "", blob)
  blob <- gsub("//", "/", blob)
  blob <- gsub("//", "/", blob)

  if (missing(contents) && missing(file)) stop("Content or file needs to be supplied")

  if (!missing(contents) && !missing(file))stop("Provided either Content OR file Argument")


  if (missing(storageKey) && !missing(azureActiveContext) && !is.null(azureActiveContext)) {
    storageKey <- refreshStorageKey(azureActiveContext, storageAccount, resourceGroup)
  }

  URL <- paste0("http://", storageAccount, ".blob.core.windows.net/", container, "/", blob)

  r <- callAzureStorageApi(URL, verb = "PUT",
    headers = "x-ms-blob-type:Blockblob",
    CMD = paste0("/", blob),
    content = contents,
    contenttype = "text/plain; charset=UTF-8",
    storageKey = storageKey, storageAccount = storageAccount, container = container,
    verbose = verbose)

  if (status_code(r) == 404) {
    warning("Azure response: container not found")
    return(NULL)
  }
  stopWithAzureError(r)

  updateAzureActiveContext(azureActiveContext, blob = blob)
  message("blob ", blob, " saved: ", nchar(contents), " bytes written")
  TRUE
}




#' Find file in a storage account directory.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @inheritParams azureSAGetKey
#' @inheritParams azurePutBlob
#'
#' @family Blob store functions
#' @export
azureBlobFind <- function(azureActiveContext, file, storageAccount, storageKey,
                          container, resourceGroup, verbose = FALSE) {

  if (!missing(azureActiveContext) && !is.null(azureActiveContext)) {
    assert_that(is.azureActiveContext(azureActiveContext))
    azureCheckToken(azureActiveContext)
      if (missing(storageAccount)) storageAccount <- azureActiveContext$storageAccount
      if (missing(storageKey)) storageKey <- azureActiveContext$storageKey
      if (missing(container)) container <- azureActiveContext$container
      if (missing(resourceGroup)) resourceGroup <- azureActiveContext$resourceGroup
      storageKey <- refreshStorageKey(azureActiveContext, storageAccount, resourceGroup)
  } else {
    if (missing(container)) container <- ""
  }

  assert_that(is_resource_group(resourceGroup))
  assert_that(is_storage_account(storageAccount))
  assert_that(is_container(container))
  assert_that(is_storage_key(storageKey))

  if (missing(file)) {
    stop("Error: No filename{pattern} provided")
  }
  verbosity <- set_verbosity(verbose)

  F2 <- data.frame()
  for (CI in container) {
    blobs <- azureListStorageBlobs(azureActiveContext, container = CI)
    blobs$name <- paste0("/", blobs$name)
    id_indir <- grep(file, blobs$name)
    blobs <- blobs[id_indir, 1:4]
    blobs <- cbind(container = CI, blobs)
    F2 <- rbind(F2, blobs)
  }
  rownames(F2) <- NULL
  return(F2)
}


#' Azure blob change current directory.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @inheritParams azureSAGetKey
#' @inheritParams azureBlobLS
#' @inheritParams azurePutBlob
#'
#' @family Blob store functions
#' @export
azureBlobCD <- function(azureActiveContext, directory, container, file,
                        storageAccount, storageKey, resourceGroup, verbose = FALSE) {
  if (!missing(azureActiveContext)) {
    assert_that(is.azureActiveContext(azureActiveContext))
    azureCheckToken(azureActiveContext)
    if (missing(storageAccount)) storageAccount <- azureActiveContext$storageAccount
    if (missing(storageKey)) storageKey <- azureActiveContext$storageKey
    if (missing(container)) container <- azureActiveContext$container
    if (missing(resourceGroup)) resourceGroup <- azureActiveContext$resourceGroup
    storageKey <- refreshStorageKey(azureActiveContext, storageAccount, resourceGroup)
  } else {
    if (missing(directory)) directory <- "/"
    if (missing(container)) container <- ""
  }

  verbosity <- set_verbosity(verbose)

  assert_that(is_resource_group(resourceGroup))
  assert_that(is_storage_account(storageAccount))
  assert_that(is_container(container))
  assert_that(is_storage_key(storageKey))

  if (missing(directory)) {
    directory <- azureActiveContext$directory
    container <- azureActiveContext$container
    if (length(directory) < 1)
      directory <- "/"  # No previous Dir value
    if (length(container) < 1) {
      directory <- "/"  # No previous Dir value
      container <- ""
    } else if (container != container)
      directory <- "/"  # Change of container

    updateAzureActiveContext(azureActiveContext,
        storageAccount = storageAccount,
        resourceGroup = resourceGroup,
        storageKey = storageKey,
        container = container,
        directory = directory
      )
    return(paste0("Current directory - ", storageAccount, " >  ", container, " : ", directory))
  }

  storageKey <- refreshStorageKey(azureActiveContext, storageAccount, resourceGroup)
  
  if (length(storageKey) < 1) {
    stop("Error: No storageKey provided: Use storageKey argument or set in AzureContext")
  }

  if (directory == "../" || directory == "..") {
    # Basic attempt azToken relative paths
    directory <- gsub("/[a-zA-Z0-9]*$", "", azureActiveContext$directory)
  }

  if (directory == "../..") {
    directory <- gsub("/[a-zA-Z0-9]*$", "", azureActiveContext$directory)
    directory <- gsub("/[a-zA-Z0-9]*$", "", directory)
  }

  if (directory == "../../..") {
    directory <- gsub("/[a-zA-Z0-9]*$", "", azureActiveContext$directory)
    directory <- gsub("/[a-zA-Z0-9]*$", "", directory)
    directory <- gsub("/[a-zA-Z0-9]*$", "", directory)
  }

  updateAzureActiveContext(azureActiveContext,
    storageAccount = storageAccount,
    resourceGroup = resourceGroup,
    storageKey = storageKey,
    container = container,
    directory = directory
  )

  paste0("Current directory - ", storageAccount, " >  ", container, " : ", directory)
}

#' Delete a specifed storage blob.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @inheritParams azureSAGetKey
#' @inheritParams azureBlobLS
#'
#' @family Blob store functions
#' @export

azureDeleteBlob <- function(azureActiveContext, blob, directory,
                            storageAccount, storageKey, container, resourceGroup, verbose = FALSE) {
  if (!missing(azureActiveContext)) {
    assert_that(is.azureActiveContext(azureActiveContext))
    azureCheckToken(azureActiveContext)
    if (missing(storageAccount)) storageAccount <- azureActiveContext$storageAccount
    if (missing(storageKey)) storageKey <- azureActiveContext$storageKey
    if (missing(container)) container <- azureActiveContext$container
    if (missing(resourceGroup)) resourceGroup <- azureActiveContext$resourceGroup
    if (missing(blob)) blob <- azureActiveContext$blob  
    storageKey <- refreshStorageKey(azureActiveContext, storageAccount, resourceGroup)
  }
  assert_that(is_resource_group(resourceGroup))
  assert_that(is_storage_account(storageAccount))
  assert_that(is_container(container))
  assert_that(is_storage_key(storageKey))
  assert_that(is_blob(blob))

  verbosity <- set_verbosity(verbose)

  directory <- azureActiveContext$directory
  container <- azureActiveContext$container

  if (missing(directory)) {
    if (length(directory) < 1)
      directory <- ""  # No previous Dir value
    if (length(container) < 1) {
      directory <- ""  # No previous Dir value
      container <- ""
    } else if (container != container)
      directory <- ""  # Change of container
  } else directory <- directory

  if (nchar(directory) > 0)
    directory <- paste0(directory, "/")

  blob <- paste0(directory, blob)
  blob <- gsub("^/", "", blob)
  blob <- gsub("^\\./", "", blob)

  URL <- paste0("http://", storageAccount, ".blob.core.windows.net/", container, "/", blob)

  xdate <- x_ms_date()
  SIG <- getSig(azureActiveContext, url = URL, verb = "DELETE", key = storageKey,
                storageAccount = storageAccount, container = container,
                CMD = paste0("/", blob), date = xdate)

  sharedKey <- paste0("SharedKey ", storageAccount, ":", SIG)

  r <- DELETE(URL, azure_storage_header(sharedKey, date = xdate), verbosity)

  stopWithAzureError(r)
  if (status_code(r) == 202) message("Blob delete request accepted")
  return(TRUE)
}
