
#' List storage blobs for specified storage account.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @inheritParams azureSAGetKey
#'
#' @family blob store functions
#' @export
azureListStorageBlobs <- function(azureActiveContext, storageAccount, storageKey,
                             container, resourceGroup, subscriptionID,
                             azToken, verbose = FALSE) {

  #if (!missing(azureActiveContext)) {
    #azureCheckToken(azureActiveContext)
  #}

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

  if (length(resourceGroup) < 1) {
    stop("Error: No resourceGroup provided: Use resourceGroup argument or set in AzureContext")
  }
  if (length(storageAccount) < 1) {
    stop("Error: No storageAccount provided: Use storageAccount argument or set in AzureContext")
  }
  if (length(container) < 1) {
    stop("Error: No container provided: Use container argument or set in AzureContext")
  }

  if (missing(storageKey) && !missing(azureActiveContext) && !is.null(azureActiveContext)) {
    storageKey <- refreshStorageKey(azureActiveContext, storageAccount, resourceGroup)
  }

    if (length(storageKey) < 1) {
      stop("Error: No storageKey provided: Use storageKey argument or set in AzureContext")
    }
  

  URL <- paste0("http://", storageAccount, ".blob.core.windows.net/", container, "?restype=container&comp=list")
  D1 <- Sys.getlocale("LC_TIME")
  Sys.setlocale("LC_TIME", "us")
  Sys.setlocale("LC_TIME", D1)
  D1 <- format(Sys.time(), "%a, %d %b %Y %H:%M:%S %Z", tz = "GMT")

  SIG <- getSig(azureActiveContext, url = URL, verb = "GET", key = storageKey,
                storageAccount = storageAccount, container = container,
                CMD = "\ncomp:list\nrestype:container", dateS = D1)
  
  AT <- paste0("SharedKey ", storageAccount, ":", SIG)
  r <- GET(URL, add_headers(.headers = c(Authorization = AT, `Content-Length` = "0",
                                         `x-ms-version` = "2015-04-05",
                                         `x-ms-date` = D1)),
           verbosity)


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


  getXmlBlob <- function(x, property, path = "//blobs//blob//properties/"){
    pth <- paste0(path, property)
    xpathSApply(y, pth, xmlValue)
  }


  azureActiveContext$storageAccount <- storageAccount
  azureActiveContext$resourceGroup <- resourceGroup
  azureActiveContext$storageKey <- storageKey

  data.frame(
    name            = getXmlBlob(y, "name", path = "//blobs//blob//"),
    lastModified    = getXmlBlob(y, "last-modified"),
    length          = getXmlBlob(y, "content-length"),
    type            = getXmlBlob(y, "blobtype"),
    leaseState      = getXmlBlob(y, "leasestate"),
    stringsAsFactors = FALSE,
    check.names = FALSE
  )
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
                        storageAccount, storageKey, container, resourceGroup, subscriptionID,
                        azToken, verbose = FALSE) {
  azureCheckToken(azureActiveContext)
  if (missing(subscriptionID)) {
    subscriptionID <- azureActiveContext$subscriptionID
  } else (subscriptionID <- subscriptionID)
  if (missing(azToken)) {
    azToken <- azureActiveContext$Token
  } else (azToken <- azToken)
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
    DC <- azureActiveContext$Dcontainer
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
  azureActiveContext$Dcontainer <- container
  cat(paste0("Current directory - ", storageAccount, " >  ", container, " : ", DIR, "\n\n"))

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
    azureActiveContext$Dcontainer <- container

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

#' @param type String, either "text" or "raw"
#'
#' @family blob store functions
#' @export

azureGetBlob <- function(azureActiveContext, blob, directory, type = "text",
                         storageAccount, storageKey, container, resourceGroup, subscriptionID,
                         azToken, verbose = FALSE) {
  #if (!missing(azureActiveContext) || !is.null(azureActiveContext)) {
    #azureCheckToken(azureActiveContext)
  #}

  #if (missing(subscriptionID)) {
    #subscriptionID <- azureActiveContext$subscriptionID
  #} 
  #if (missing(azToken)) {
    #azToken <- azureActiveContext$Token
  #} 

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

  #storageKey <- refreshStorageKey(azureActiveContext, storageAccount, resourceGroup)

  if (length(storageKey) < 1) {
    stop("Error: No storageKey provided: Use storageKey argument or set in AzureContext")
  }


  if (missing(directory)) {
    directory <- azureActiveContext$directory
  }

  if (missing(container)) {
    container <- azureActiveContext$Dcontainer
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
  cat(blob)

  URL <- paste("http://", storageAccount, ".blob.core.windows.net/", container, "/",
               blob, sep = "")


  D1 <- Sys.getlocale("LC_TIME")
  Sys.setlocale("LC_TIME", "C")
  # `x-ms-date` <- format(Sys.time(),'%a, %d %b %Y %H:%M:%S %Z',
  # tz='GMT')
  Sys.setlocale("LC_TIME", D1)
  D1 <- format(Sys.time(), "%a, %d %b %Y %H:%M:%S %Z", tz = "GMT")

  SIG <- getSig(azureActiveContext, url = URL, verb = "GET", key = storageKey,
                storageAccount = storageAccount, container = container,
                CMD = paste0("/", blob), dateS = D1)

  AT <- paste0("SharedKey ", storageAccount, ":", SIG)

  r <- GET(URL, add_headers(.headers = c(Authorization = AT, `Content-Length` = "0",
                                         `x-ms-version` = "2015-04-05",
                                         `x-ms-date` = D1)),
           verbosity)

  if (status_code(r) == 404) {
    cat(blob)
    warning("file not found")
    return(NULL)
  } else if (status_code(r) != 200)
    stopWithAzureError(r)

  r2 <- content(r, type, encoding = "UTF-8")

  azureActiveContext$storageAccount <- storageAccount
  azureActiveContext$resourceGroup <- resourceGroup
  azureActiveContext$storageKey <- storageKey
  azureActiveContext$container <- container
  azureActiveContext$blob <- blob
  return(r2)
}


#' Write contents to a specifed Storage blob.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @inheritParams azureSAGetKey
#' @inheritParams azureBlobLS
#'
#' @param contents - Object to Store or Value
#' @param file - Local filename to Store in Azure blob
#'
#' @family blob store functions
#' @export
azurePutBlob <- function(azureActiveContext, blob, contents = "", file = "",
                         directory, storageAccount, storageKey,
                         container, resourceGroup, subscriptionID,
                         azToken, verbose = FALSE) {

  azureCheckToken(azureActiveContext)

  if (missing(subscriptionID)) {
    subscriptionID <- azureActiveContext$subscriptionID
  } else (subscriptionID <- subscriptionID)
  if (missing(azToken)) {
    azToken <- azureActiveContext$Token
  } else (azToken <- azToken)
  if (missing(resourceGroup)) {
    resourceGroup <- azureActiveContext$resourceGroup
  } else (resourceGroup <- resourceGroup)
  if (missing(storageAccount)) {
    storageAccount <- azureActiveContext$storageAccount
  } else (storageAccount <- storageAccount)
  if (missing(storageKey)) {
    storageKey <- azureActiveContext$storageKey
  } else (storageKey <- storageKey)
  if (missing(container)) {
    container <- azureActiveContext$container
  } else (container <- container)
  if (missing(blob)) {
    blob <- azureActiveContext$blob
  } else (blob <- blob)
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
  DC <- azureActiveContext$Dcontainer

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


  storageKey <- refreshStorageKey(azureActiveContext, storageAccount, resourceGroup)

  if (length(storageKey) < 1) {
    stop("Error: No storageKey provided: Use storageKey argument or set in AzureContext")
  }

  URL <- paste("http://", storageAccount, ".blob.core.windows.net/", container, "/",
               blob, sep = "")

  D1 <- Sys.getlocale("LC_TIME")
  Sys.setlocale("LC_TIME", "C")
  Sys.setlocale("LC_TIME", D1)
  D1 <- format(Sys.time(), "%a, %d %b %Y %H:%M:%S %Z", tz = "GMT")
  if (nchar(contents) == 0)
    contents <- "-"

  SIG <- getSig(azureActiveContext, url = URL, verb = "PUT", key = storageKey,
                storageAccount = storageAccount, container = container,
                contenttype = "text/plain; charset=UTF-8",
                size = nchar(contents),
                headers = "x-ms-blob-type:Blockblob",
                CMD = paste0("/", blob), dateS = D1)



  AT <- paste0("SharedKey ", storageAccount, ":", SIG)

  r <- PUT(URL, add_headers(.headers = c(Authorization = AT,
                                         `Content-Length` = nchar(contents),
                                         `x-ms-version` = "2015-04-05",
                                         `x-ms-date` = D1,
                                         `x-ms-blob-type` = "Blockblob",
                                         `Content-type` = "text/plain; charset=UTF-8")),
           body = contents,
           verbosity)

  azureActiveContext$blob <- blob
  return(paste("blob:", blob, " Saved:", nchar(contents), "bytes written"))
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
                          container, resourceGroup, subscriptionID,
                          azToken, verbose = FALSE) {
  azureCheckToken(azureActiveContext)
  if (missing(subscriptionID)) {
    subscriptionID <- azureActiveContext$subscriptionID
  } else (subscriptionID <- subscriptionID)
  if (missing(azToken)) {
    azToken <- azureActiveContext$Token
  } else (azToken <- azToken)
  if (missing(resourceGroup)) {
    resourceGroup <- azureActiveContext$resourceGroup
  } else (resourceGroup <- resourceGroup)
  if (missing(storageAccount)) {
    storageAccount <- azureActiveContext$storageAccount
  } else (storageAccount <- storageAccount)
  if (missing(storageKey)) {
    storageKey <- azureActiveContext$storageKey
  } else (storageKey <- storageKey)
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
                        storageAccount, storageKey, resourceGroup, subscriptionID,
                        azToken,verbose = FALSE) {
  azureCheckToken(azureActiveContext)
  if (missing(subscriptionID)) {
    subscriptionID <- azureActiveContext$subscriptionID
  } else (subscriptionID <- subscriptionID)
  if (missing(azToken)) {
    azToken <- azureActiveContext$Token
  } else (azToken <- azToken)
  if (missing(resourceGroup)) {
    resourceGroup <- azureActiveContext$resourceGroup
  } else (resourceGroup <- resourceGroup)
  if (missing(storageAccount)) {
    storageAccount <- azureActiveContext$storageAccount
  } else (storageAccount <- storageAccount)
  if (missing(storageKey)) {
    storageKey <- azureActiveContext$storageKey
  } else (storageKey <- storageKey)
  if (missing(container)) {
    container <- azureActiveContext$container
  } else (container <- container)
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
    DC <- azureActiveContext$Dcontainer
    if (length(DIR) < 1)
      DIR <- "/"  # No previous Dir value
    if (length(DC) < 1) {
      DIR <- "/"  # No previous Dir value
      DC <- ""
    } else if (container != DC)
      DIR <- "/"  # Change of container

    azureActiveContext$directory <- DIR
    azureActiveContext$container <- container
    azureActiveContext$storageAccount <- storageAccount
    azureActiveContext$resourceGroup <- resourceGroup
    azureActiveContext$Dcontainer <- container
    return(paste0("Current directory - ", storageAccount, " >  ", container, " : ",
                  DIR))

  }
  storageKey <- refreshStorageKey(azureActiveContext, storageAccount, resourceGroup)
  if (length(storageKey) < 1) {
    stop("Error: No storageKey provided: Use storageKey argument or set in AzureContext")
  }

  if (directory == "../" || directory == "..") {
    # Basic attempt at relazTokenve paths
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

  azureActiveContext$directory <- directory
  azureActiveContext$container <- container
  azureActiveContext$Dcontainer <- container
  azureActiveContext$storageAccount <- storageAccount
  azureActiveContext$resourceGroup <- resourceGroup
  azureActiveContext$Dcontainer <- container

  return(paste0("Current directory - ", storageAccount, " >  ", container, " : ", directory))
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
                            storageAccount, storageKey, container, resourceGroup, subscriptionID,
                            azToken, verbose = FALSE) {
  azureCheckToken(azureActiveContext)
  if (missing(subscriptionID)) {
    subscriptionID <- azureActiveContext$subscriptionID
  } else (subscriptionID <- subscriptionID)
  if (missing(azToken)) {
    azToken <- azureActiveContext$Token
  } else (azToken <- azToken)
  if (missing(resourceGroup)) {
    resourceGroup <- azureActiveContext$resourceGroup
  } else (resourceGroup <- resourceGroup)
  if (missing(storageAccount)) {
    storageAccount <- azureActiveContext$storageAccount
  } else (storageAccount <- storageAccount)
  if (missing(storageKey)) {
    storageKey <- azureActiveContext$storageKey
  } else (storageKey <- storageKey)
  if (missing(container)) {
    container <- azureActiveContext$container
  } 
  if (missing(blob)) {
    blob <- azureActiveContext$blob
  } else (blob <- blob)
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
  DC <- azureActiveContext$Dcontainer

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
  cat(blob)

  URL <- paste("http://", storageAccount, ".blob.core.windows.net/", container, "/",
               blob, sep = "")


  D1 <- Sys.getlocale("LC_TIME")
  Sys.setlocale("LC_TIME", "C")
  Sys.setlocale("LC_TIME", D1)
  D1 <- format(Sys.time(), "%a, %d %b %Y %H:%M:%S %Z", tz = "GMT")

  SIG <- getSig(azureActiveContext, url = URL, verb = "DELETE", key = storageKey,
                storageAccount = storageAccount, container = container,
                CMD = paste0("/", blob), dateS = D1)

  AT <- paste0("SharedKey ", storageAccount, ":", SIG)

  r <- DELETE(URL, add_headers(.headers = c(Authorization = AT,
                                            `Content-Length` = "0",
                                            `x-ms-version` = "2015-04-05",
                                            `x-ms-date` = D1)),
              verbosity)

  if (status_code(r) == 202) {
    return("Blob delete request accepted")
  }
  else
    stopWithAzureError(r)
}
