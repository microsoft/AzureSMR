
#' List storage blobs for specified storage account.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @inheritParams azureSAGetKey
#'
#' @param maxresults Optional. Specifies the maximum number of blobs to return, including all BlobPrefix elements. If the request does not specify maxresults or specifies a value greater than 5,000, the server will return up to 5,000 items.  Setting maxresults to a value less than or equal to zero results in error response code 400 (Bad Request).
#' @param prefix Optional. Filters the results to return only blobs whose names begin with the specified prefix.
#' @param delimiter Optional. When the request includes this parameter, the operation returns a BlobPrefix element in the response body that acts as a placeholder for all blobs whose names begin with the same substring up to the appearance of the delimiter character. The delimiter may be a single character or a string.
#' @param marker Optional. A string value that identifies the portion of the list to be returned with the next list operation. The operation returns a marker value within the response body if the list returned was not complete. The marker value may then be used in a subsequent call to request the next set of list items.  The marker value is opaque to the client.
#'
#' @return Returns a data frame. This data frame has an attribute called "marker" that can be used with the "marker" argument to return the next set of values.
#' @family blob store functions
#' @export
azureListStorageBlobs <- function(azureActiveContext, storageAccount, storageKey,
                             container, resourceGroup, maxresults, prefix, delimiter, marker, verbose = FALSE) {

  if (!missing(azureActiveContext)) azureCheckToken(azureActiveContext)

  if (missing(resourceGroup) && !missing(azureActiveContext)) {
    resourceGroup <- azureActiveContext$resourceGroup
  } 

  if (missing(storageAccount) && !missing(azureActiveContext) && !is.null(azureActiveContext)) {
    storageAccount <- azureActiveContext$storageAccount
  } 
  if (missing(storageKey) && !missing(azureActiveContext)) {
    storageKey <- azureActiveContext$storageKey
  } 
  if (missing(container) && !missing(azureActiveContext)) {
    container <- azureActiveContext$container
  } 
  verbosity <- if (verbose)
    httr::verbose(TRUE) else NULL

  if (missing(storageKey) && !missing(azureActiveContext) && !is.null(azureActiveContext)) {
    storageKey <- refreshStorageKey(azureActiveContext, storageAccount, resourceGroup)
  }

  validateStorageArguments(resourceGroup = resourceGroup, 
    storageAccount = storage.mode,
    container = container, 
    storageKey = storage.mode
  )

  URL <- paste0("http://", storageAccount, ".blob.core.windows.net/", container, "?restype=container&comp=list")
  if (!missing(maxresults)  && !is.null(maxresults)) URL <- paste0(URL, "&maxresults=", maxresults)
  if (!missing(prefix) && !is.null(prefix)) URL <- paste0(URL, "&prefix=", prefix)
  if (!missing(delimiter) && !is.null(delimiter)) URL <- paste0(URL, "&delimiter=", delimiter)
  if (!missing(marker) && !is.null(marker)) URL <- paste0(URL, "&marker=", marker)
  r <- callAzureStorageApi(URL, 
    storageKey = storageKey, storageAccount = storageAccount, container = container,
    verbose = verbose)

  if (status_code(r) == 404) {
    warning("Azure response: container not found")
    return(NULL)
  } else {
    if (status_code(r) != 200)
      stopWithAzureError(r)
  }

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
    resourceGroup = resourceGroup,
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


#' List blob files in a Storage account directory.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @inheritParams azureSAGetKey

#' @param directory Blob store directory to list for content
#' @param recursive If TRUE, list blob store directories recursively
#'
#' @family blob store functions
#' @export
azureBlobLS <- function(azureActiveContext, directory, recursive = FALSE,
                        storageAccount, storageKey, container, resourceGroup, verbose = FALSE) {

  if (!missing(azureActiveContext)) azureCheckToken(azureActiveContext)

  if (missing(resourceGroup)) {
    resourceGroup <- azureActiveContext$resourceGroup
  } else (resourceGroup <- resourceGroup)
  if (missing(storageKey)) {
    storageKey <- azureActiveContext$storageKey
  } else (storageKey <- storageKey)
  if (missing(storageAccount)) {
    storageAccount <- azureActiveContext$storageAccount
  } else (storageAccount <- storageAccount)
  if (missing(container)) {
    container <- azureActiveContext$container
  } else (container <- container)
  if (missing(directory)) {
    DIR <- azureActiveContext$directory
  } else (DIR <- directory)
  verbosity <- if (verbose)
    httr::verbose(TRUE) else NULL

  if (length(resourceGroup) < 1) {
    stop("Error: No resourceGroup provided: Use resourceGroup argument or set in AzureContext")
  }
  if (length(storageAccount) < 1) {
    stop("Error: No storageAccount provided: Use storageAccount argument or set in AzureContext")
  }
  if (length(container) < 1) {
    stop("Error: No container provided: Use container argument or set in AzureContext")
  }
  SD <- 0

  if (missing(directory)) {
    DIR <- azureActiveContext$directory
    DC <- azureActiveContext$container
    if (length(DC) < 1)
      DC <- ""
    if (length(DIR) < 1)
      DIR <- "/"
    if (DC != container)
      DIR <- "/"  # Change of container
  } else {
    if (substr(directory, 1, 1) != "/") {
      DIR2 <- azureActiveContext$directory
      if (length(DIR2) > 0) {
        DIR <- paste0(DIR2, "/", directory)
        SD <- 1
        DIR <- gsub("\\./", "", DIR)
      }
    }
  }

  if (length(DIR) < 1) {
    DIR <- "/"
  }
  if (substr(DIR, 1, 1) != "/")
    DIR <- paste0("/", DIR)

  DIR <- gsub("//", "/", DIR)

  if (!missing(azureActiveContext) && !is.null(azureActiveContext) && missing(storageKey)) {
    storageKey <- refreshStorageKey(azureActiveContext, storageAccount, resourceGroup)
  }

  if (length(storageKey) < 1) {
    stop("Error: No storageKey provided: Use storageKey argument or set in AzureContext")
  }

  azureActiveContext$Dircontainer <- container

  files <- azureListStorageBlobs(azureActiveContext, container = container)

  files$name <- paste0("/", files$name)
  files$name <- gsub("//", "/", files$name)
  files$name <- gsub("//", "/", files$name)

  F1 <- grep(paste0("^", DIR), files$name)

  if (SD == 0)
    azureActiveContext$directory <- DIR
  azureActiveContext$container <- container
  azureActiveContext$container <- container
  message(paste0("Current directory - ", storageAccount, " >  ", container, " : ", DIR, "\n\n"))

  DIR <- gsub("//", "/", DIR)
  Depth <- length(strsplit(DIR, "/")[[1]])
  FO <- data.frame()
  Prev <- ""
  if (recursive == TRUE) {
    files <- files[F1, ]
    return(files)
  } else {
    files <- files[F1, ]
    rownames(files) <- NULL
    F2 <- strsplit(files$name, "/")
    f1 <- 1
    f2 <- 1
    for (RO in F2) {
      if (length(RO) > Depth) {
        if (length(RO) >= Depth + 1) {
          # Check Depth Level
          if (Prev != RO[Depth + 1]) {
            if (length(RO) > Depth + 1) {
              FR <- data.frame(paste0("./", RO[Depth + 1]), "directory",
                               files[f1, 2:4], stringsAsFactors = FALSE)
              FR[, 4] <- "-"  # Second file name found so assumed blob was a directory

            } else FR <- data.frame(paste0("./", RO[Depth + 1]),
                                    "file", files[f1, 2:4], stringsAsFactors = FALSE)

            colnames(FR)[2] <- "type"

            FO <- rbind(FO, FR)

            f2 <- f2 + 1
          } else {
            FO[f2 - 1, 2] <- "directory"  # Second file name found so assumed blob was a directory
            FO[f2 - 1, 4] <- "-"  # Second file name found so assumed blob was a directory
          }
        }
      }
      Prev <- RO[Depth + 1]
      if (is.na(Prev))
        Prev <- ""
      f1 <- f1 + 1
    }
    if (f2 == 1) {
      if (SD == 0)
        warning("directory not found") else warning("No files found")
      return(NULL)
    }
    azureActiveContext$directory <- DIR
    azureActiveContext$container <- container
    azureActiveContext$storageAccount <- storageAccount
    azureActiveContext$resourceGroup <- resourceGroup
    azureActiveContext$container <- container

    rownames(FO) <- NULL
    colnames(FO)[1] <- "filename"
    colnames(FO)[2] <- "type"
    FN <- grep("directory", FO$type)  # Suffix / to directory names
    FO$filename[FN] <- paste0(FO$filename[FN], "/")
    return(FO)
  }
}


#' Get contents from a specifed Storage blob.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @inheritParams azureSAGetKey
#' @inheritParams azureBlobLS

#' @param type String, either "text" or "raw". Passed to `httr::content`
#'
#' @family blob store functions
#' @export

azureGetBlob <- function(azureActiveContext, blob, directory, type = "text",
                         storageAccount, storageKey, container, resourceGroup, verbose = FALSE) {
  if (!missing(azureActiveContext)) azureCheckToken(azureActiveContext)

  if (missing(resourceGroup) && !missing(azureActiveContext)) {
    resourceGroup <- azureActiveContext$resourceGroup
  }

  if (missing(storageAccount) && !missing(azureActiveContext) && !is.null(azureActiveContext)) {
    storageAccount <- azureActiveContext$storageAccount
  }
  if (missing(storageKey) && !missing(azureActiveContext)) {
    storageKey <- azureActiveContext$storageKey
  }
  if (missing(container) && !missing(azureActiveContext)) {
    container <- azureActiveContext$container
  }
  if (missing(blob) && !missing(azureActiveContext)) {
    blob <- azureActiveContext$blob
  } 
  verbosity <- if (verbose)
    httr::verbose(TRUE) else NULL

  if (length(resourceGroup) < 1) {
    stop("Error: No resourceGroup provided: Use resourceGroup argument or set in AzureContext")
  }
  if (length(storageAccount) < 1) {
    stop("Error: No storageAccount provided: Use storageAccount argument or set in AzureContext")
  }
  if (length(container) < 1) {
    stop("Error: No container provided: Use container argument or set in AzureContext")
  }
  if (length(blob) < 1) {
    stop("Error: No blob provided: Use blob argument or set in AzureContext")
  }

  if (length(storageKey) < 1) {
    stop("Error: No storageKey provided: Use storageKey argument or set in AzureContext")
  }


  if (missing(directory)) {
    directory <- azureActiveContext$directory
  }

  if (missing(container)) {
    container <- azureActiveContext$container
  }

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
  } else if (status_code(r) != 200)
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


#' Write contents to a specifed Storage blob.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @inheritParams azureSAGetKey
#' @inheritParams azureBlobLS
#'
#' @param contents - Object or value to store
#' @param file - Local filename to store in Azure blob
#'
#' @family blob store functions
#' @export
azurePutBlob <- function(azureActiveContext, blob, contents = "", file = "",
                         directory, storageAccount, storageKey,
                         container, resourceGroup, verbose = FALSE) {

  if (!missing(azureActiveContext)) azureCheckToken(azureActiveContext)

  if (missing(resourceGroup)) {
    resourceGroup <- azureActiveContext$resourceGroup
  }
  if (missing(storageAccount)) {
    storageAccount <- azureActiveContext$storageAccount
  }
  if (missing(storageKey)) {
    STK <- azureActiveContext$storageKey
  } else (STK <- storageKey)
  if (missing(container)) {
    container <- azureActiveContext$container
  }
  if (missing(blob)) {
    blob <- azureActiveContext$blob
  }
  verbosity <- if (verbose)
    httr::verbose(TRUE) else NULL

  if (length(resourceGroup) < 1) {
    stop("Error: No resourceGroup provided: Use resourceGroup argument or set in AzureContext")
  }
  if (length(storageAccount) < 1) {
    stop("Error: No storageAccount provided: Use storageAccount argument or set in AzureContext")
  }
  if (length(container) < 1) {
    stop("Error: No container provided: Use container argument or set in AzureContext")
  }
  if (length(blob) < 1) {
    stop("Error: No blob provided: Use blob argument or set in AzureContext")
  }

  DIR <- azureActiveContext$directory
  DC <- azureActiveContext$container

  if (missing(directory)) {

    if (length(DIR) < 1)
      DIR <- ""  # No previous Dir value
    if (length(DC) < 1) {
      DIR <- ""  # No previous Dir value
      DC <- ""
    } else if (container != DC)
      DIR <- ""  # Change of container
  } else DIR <- directory
  if (nchar(DIR) > 0)
    DIR <- paste0(DIR, "/")

  blob <- paste0(DIR, blob)
  blob <- gsub("^/", "", blob)
  blob <- gsub("//", "/", blob)
  blob <- gsub("//", "/", blob)

  if (missing(contents) && missing(file))
    stop("Content or file needs to be supplied")

  if (!missing(contents) && !missing(file))
    stop("Provided either Content OR file Argument")


  if (missing(storageKey) && !missing(azureActiveContext) && !is.null(azureActiveContext)) {
    storageKey <- refreshStorageKey(azureActiveContext, storageAccount, resourceGroup)
  }

  if (length(storageKey) < 1) {
    stop("Error: No storageKey provided: Use storageKey argument or set in AzureContext")
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
  } else {
    if (!status_code(r) %in% c(200, 201))
      stopWithAzureError(r)
    }

  updateAzureActiveContext(azureActiveContext, blob = blob)
  message("blob:", blob, " Saved:", nchar(contents), "bytes written")
  TRUE
}




#' Find file in a Storage account directory.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @inheritParams azureSAGetKey
#' @inheritParams azurePutBlob
#'
#' @family blob store functions
#' @export
azureBlobFind <- function(azureActiveContext, file, storageAccount, storageKey,
                          container, resourceGroup, verbose = FALSE) {

  if (!missing(azureActiveContext)) azureCheckToken(azureActiveContext)

  if (missing(resourceGroup)) {
    resourceGroup <- azureActiveContext$resourceGroup
  }
  if (missing(storageAccount)) {
    storageAccount <- azureActiveContext$storageAccount
  }
  if (missing(storageKey)) {
    storageKey <- azureActiveContext$storageKey
  }
  if (missing(file)) {
    stop("Error: No filename{pattern} provided")
  }
  verbosity <- if (verbose)
    httr::verbose(TRUE) else NULL

  if (length(resourceGroup) < 1) {
    stop("Error: No resourceGroup provided: Use resourceGroup argument or set in AzureContext")
  }
  if (length(storageAccount) < 1) {
    stop("Error: No storageAccount provided: Use storageAccount argument or set in AzureContext")
  }


  storageKey <- refreshStorageKey(azureActiveContext, storageAccount, resourceGroup)

  if (length(storageKey) < 1) {
    stop("Error: No storageKey provided: Use storageKey argument or set in AzureContext")
  }

  if (!missing(container)) {
    CL <- container
  } else {
    CL <- azureListStorageContainers(azureActiveContext)$name
  }

  F2 <- data.frame()
  for (CI in CL) {
    files <- azureListStorageBlobs(azureActiveContext, container = CI)
    files$name <- paste0("/", files$name)
    F1 <- grep(file, files$name)
    files <- files[F1, 1:4]
    files <- cbind(container = CI, files)
    F2 <- rbind(F2, files)
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
#' @family blob store functions
#' @export
azureBlobCD <- function(azureActiveContext, directory, container, file,
                        storageAccount, storageKey, resourceGroup, verbose = FALSE) {
  if (!missing(azureActiveContext)) azureCheckToken(azureActiveContext)
   
  if (missing(resourceGroup)) {
    resourceGroup <- azureActiveContext$resourceGroup
  }
  if (missing(storageAccount)) {
    storageAccount <- azureActiveContext$storageAccount
  }
  if (missing(storageKey)) {
    storageKey <- azureActiveContext$storageKey
  }
  if (missing(container)) {
    container <- azureActiveContext$container
  }
  verbosity <- if (verbose)
    httr::verbose(TRUE) else NULL

  if (length(resourceGroup) < 1) {
    stop("Error: No resourceGroup provided: Use resourceGroup argument or set in AzureContext")
  }
  if (length(storageAccount) < 1) {
    stop("Error: No storageAccount provided: Use storageAccount argument or set in AzureContext")
  }

  if (missing(directory)) {
    DIR <- azureActiveContext$directory
    DC <- azureActiveContext$container
    if (length(DIR) < 1)
      DIR <- "/"  # No previous Dir value
    if (length(DC) < 1) {
      DIR <- "/"  # No previous Dir value
      DC <- ""
    } else if (container != DC)
      DIR <- "/"  # Change of container

    updateAzureActiveContext(azureActiveContext,
        storageAccount = storageAccount,
        resourceGroup = resourceGroup,
        storageKey = storageKey,
        container = container,
        directory = DIR
      )
    return(paste0("Current directory - ", storageAccount, " >  ", container, " : ", DIR))
  }

  storageKey <- refreshStorageKey(azureActiveContext, storageAccount, resourceGroup)
  
  if (length(storageKey) < 1) {
    stop("Error: No storageKey provided: Use storageKey argument or set in AzureContext")
  }

  if (directory == "../" || directory == "..") {
    # Basic attempt at relative paths
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

#' Delete a specifed Storage blob.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @inheritParams azureSAGetKey
#' @inheritParams azureBlobLS
#'
#' @family blob store functions
#' @export

azureDeleteBlob <- function(azureActiveContext, blob, directory,
                            storageAccount, storageKey, container, resourceGroup, verbose = FALSE) {
  azureCheckToken(azureActiveContext)
  if (missing(resourceGroup)) {
    resourceGroup <- azureActiveContext$resourceGroup
  }
  if (missing(storageAccount)) {
    storageAccount <- azureActiveContext$storageAccount
  }
  if (missing(storageKey)) {
    storageKey <- azureActiveContext$storageKey
  }
  if (missing(container)) {
    container <- azureActiveContext$container
  } 
  if (missing(blob)) {
    blob <- azureActiveContext$blob
  }
  verbosity <- if (verbose)
    httr::verbose(TRUE) else NULL

  if (length(resourceGroup) < 1) {
    stop("Error: No resourceGroup provided: Use resourceGroup argument or set in AzureContext")
  }
  if (length(storageAccount) < 1) {
    stop("Error: No storageAccount provided: Use storageAccount argument or set in AzureContext")
  }
  if (length(container) < 1) {
    stop("Error: No container provided: Use container argument or set in AzureContext")
  }
  if (length(blob) < 1) {
    stop("Error: No blob provided: Use blob argument or set in AzureContext")
  }

  storageKey <- refreshStorageKey(azureActiveContext, storageAccount, resourceGroup)

  if (length(storageKey) < 1) {
    stop("Error: No storageKey provided: Use storageKey argument or set in AzureContext")
  }

  DIR <- azureActiveContext$directory
  DC <- azureActiveContext$container

  if (missing(directory)) {
    if (length(DIR) < 1)
      DIR <- ""  # No previous Dir value
    if (length(DC) < 1) {
      DIR <- ""  # No previous Dir value
      DC <- ""
    } else if (container != DC)
      DIR <- ""  # Change of container
  } else DIR <- directory

  if (nchar(DIR) > 0)
    DIR <- paste0(DIR, "/")

  blob <- paste0(DIR, blob)
  blob <- gsub("^/", "", blob)
  blob <- gsub("^\\./", "", blob)

  URL <- paste0("http://", storageAccount, ".blob.core.windows.net/", container, "/", blob)


  D1 <- Sys.getlocale("LC_TIME")
  Sys.setlocale("LC_TIME", "C")
  Sys.setlocale("LC_TIME", D1)
  D1 <- format(Sys.time(), "%a, %d %b %Y %H:%M:%S %Z", tz = "GMT")

  SIG <- getSig(azureActiveContext, url = URL, verb = "DELETE", key = storageKey,
                storageAccount = storageAccount, container = container,
                CMD = paste0("/", blob), dateSig = D1)

  AT <- paste0("SharedKey ", storageAccount, ":", SIG)

  r <- DELETE(URL, add_headers(.headers = c(Authorization = AT,
                                            `Content-Length` = "0",
                                            `x-ms-version` = "2015-04-05",
                                            `x-ms-date` = D1)),
              verbosity)

  if (status_code(r) == 202) {
    message("Blob delete request accepted")
  }
  else {
    stopWithAzureError(r)
  }
  return(TRUE)
}
