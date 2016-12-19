
#' List Storage Containers for Specified Storage Account.
#'
#' @inheritParams SetAzureContext
#' @inheritParams AzureAuthenticate
#' @inheritParams AzureSAGetKey
#'
#' @family Blob store functions
#' @export
AzureListSABlobs <- function(AzureActiveContext, StorageAccount, StorageKey,
                             Container, ResourceGroup, SubscriptionID,
                             AzToken, verbose = FALSE) {
  AzureCheckToken(AzureActiveContext)

  if (missing(SubscriptionID)) {
    SUBIDI <- AzureActiveContext$SubscriptionID
  } else (SUBIDI <- SubscriptionID)
  if (missing(AzToken)) {
    ATI <- AzureActiveContext$Token
  } else (ATI <- AzToken)
  if (missing(ResourceGroup)) {
    RGI <- AzureActiveContext$ResourceGroup
  } else (RGI <- ResourceGroup)
  if (missing(StorageAccount)) {
    SAI <- AzureActiveContext$StorageAccount
  } else (SAI <- StorageAccount)
  if (missing(StorageKey)) {
    STK <- AzureActiveContext$StorageKey
  } else (STK <- StorageKey)
  if (missing(Container)) {
    CNTR <- AzureActiveContext$Container
  } else (CNTR <- Container)
  verbosity <- if (verbose)
    httr::verbose(TRUE) else NULL

  if (length(RGI) < 1) {
    stop("Error: No ResourceGroup provided: Use ResourceGroup argument or set in AzureContext")
  }
  if (length(SAI) < 1) {
    stop("Error: No StorageAccount provided: Use StorageAccount argument or set in AzureContext")
  }
  if (length(CNTR) < 1) {
    stop("Error: No Container provided: Use Container argument or set in AzureContext")
  }

  STK <- refreshStorageKey(AzureActiveContext)

  if (length(STK) < 1) {
    stop("Error: No StorageKey provided: Use StorageKey argument or set in AzureContext")
  }

  URL <- paste("http://", SAI, ".blob.core.windows.net/", CNTR, "?restype=container&comp=list",
               sep = "")
  D1 <- Sys.getlocale("LC_TIME")
  Sys.setlocale("LC_TIME", "us")
  # `x-ms-date` <- format(Sys.time(),'%a, %d %b %Y %H:%M:%S %Z',
  # tz='GMT')
  Sys.setlocale("LC_TIME", D1)
  D1 <- format(Sys.time(), "%a, %d %b %Y %H:%M:%S %Z", tz = "GMT")

  SIG <- GetSig(AzureActiveContext, URL, "GET", STK, SAI, Container = CNTR,
                CMD = "\ncomp:list\nrestype:container", DateS = D1)

  AT <- paste0("SharedKey ", SAI, ":", SIG)
  # GetSig <- function(AzureActiveContext,url, verb, key,
  # StorageAccount,Headers=NULL,Container=NULL,CMD=NULL,Size=NULL) {
  r <- GET(URL, add_headers(.headers = c(Authorization = AT, `Content-Length` = "0",
                                         `x-ms-version` = "2015-04-05", `x-ms-date` = D1)), verbosity)


  if (status_code(r) == 404) {
    warning("Container not found")
    return(NULL)
  } else {
    if (status_code(r) != 200)
      stopWithAzureError(r)
  }

  r <- content(r, "text", encoding = "UTF-8")

  y <- htmlParse(r, encoding = "UTF-8")

  namesx <- xpathApply(y, "//blobs//blob//name", xmlValue)

  if (length(namesx) == 0) {
    warning("Container is empty")
    return(NULL)
  }

  lmodx <- xpathApply(y, "//blobs//blob//properties/last-modified", xmlValue)
  lenx <- xpathApply(y, "//blobs//blob//properties/content-length", xmlValue)
  bltx <- xpathApply(y, "//blobs//blob//properties/blobtype", xmlValue)
  cpx <- xpathApply(y, "//blobs//blob//properties/leasestate", xmlValue)

  dfn <- as.data.frame(matrix(ncol = 5, nrow = length(namesx)))

  for (i in 1:length(namesx)) {
    dfn[i, 1] <- namesx[i]
    dfn[i, 2] <- lmodx[i]
    dfn[i, 3] <- lenx[i]
    dfn[i, 4] <- bltx[i]
    dfn[i, 5] <- cpx[i]
  }
  colnames(dfn) <- c("Name", "LastModified", "Length", "Type", "LeaseState")
  AzureActiveContext$StorageAccount <- SAI
  AzureActiveContext$ResourceGroup <- RGI
  AzureActiveContext$StorageKey <- STK
  return(dfn)
}


#' List Blob files in a Storage account directory.
#'
#' @inheritParams SetAzureContext
#' @inheritParams AzureAuthenticate
#' @inheritParams AzureSAGetKey

#' @param Directory Set Directory to list
#' @param Recursive List directories recursively (Default FALSE)
#'
#' @family Blob store functions
#' @export
AzureBlobLS <- function(AzureActiveContext, Directory, Recursive = FALSE,
                        StorageAccount, StorageKey, Container, ResourceGroup, SubscriptionID,
                        AzToken, verbose = FALSE) {
  AzureCheckToken(AzureActiveContext)
  if (missing(SubscriptionID)) {
    SUBIDI <- AzureActiveContext$SubscriptionID
  } else (SUBIDI <- SubscriptionID)
  if (missing(AzToken)) {
    ATI <- AzureActiveContext$Token
  } else (ATI <- AzToken)
  if (missing(ResourceGroup)) {
    RGI <- AzureActiveContext$ResourceGroup
  } else (RGI <- ResourceGroup)
  if (missing(StorageKey)) {
    STK <- AzureActiveContext$StorageKey
  } else (STK <- StorageKey)
  if (missing(StorageAccount)) {
    SAI <- AzureActiveContext$StorageAccount
  } else (SAI <- StorageAccount)
  if (missing(Container)) {
    CNTR <- AzureActiveContext$Container
  } else (CNTR <- Container)
  if (missing(Directory)) {
    DIR <- AzureActiveContext$Directory
  } else (DIR <- Directory)
  verbosity <- if (verbose)
    httr::verbose(TRUE) else NULL

  if (length(RGI) < 1) {
    stop("Error: No ResourceGroup provided: Use ResourceGroup argument or set in AzureContext")
  }
  if (length(SAI) < 1) {
    stop("Error: No StorageAccount provided: Use StorageAccount argument or set in AzureContext")
  }
  if (length(CNTR) < 1) {
    stop("Error: No Container provided: Use Container argument or set in AzureContext")
  }
  SD <- 0

  if (missing(Directory)) {
    DIR <- AzureActiveContext$Directory
    DC <- AzureActiveContext$DContainer
    if (length(DC) < 1)
      DC <- ""
    if (length(DIR) < 1)
      DIR <- "/"
    if (DC != CNTR)
      DIR <- "/"  # Change of Container
  } else {
    if (substr(Directory, 1, 1) != "/") {
      DIR2 <- AzureActiveContext$Directory
      if (length(DIR2) > 0) {
        DIR <- paste0(DIR2, "/", Directory)
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

  STK <- refreshStorageKey(AzureActiveContext)

  if (length(STK) < 1) {
    stop("Error: No StorageKey provided: Use StorageKey argument or set in AzureContext")
  }

  AzureActiveContext$DirContainer <- CNTR

  Files <- AzureListSABlobs(AzureActiveContext, Container = CNTR)

  Files$Name <- paste0("/", Files$Name)
  Files$Name <- gsub("//", "/", Files$Name)
  Files$Name <- gsub("//", "/", Files$Name)

  F1 <- grep(paste0("^", DIR), Files$Name)

  if (SD == 0)
    AzureActiveContext$Directory <- DIR
  AzureActiveContext$Container <- CNTR
  AzureActiveContext$DContainer <- CNTR
  cat(paste0("Current Directory - ", SAI, " >  ", CNTR, " : ", DIR, "\n\n"))

  DIR <- gsub("//", "/", DIR)
  Depth <- length(strsplit(DIR, "/")[[1]])
  FO <- data.frame()
  Prev <- ""
  if (Recursive == TRUE) {
    Files <- Files[F1, ]
    return(Files)
  } else {
    Files <- Files[F1, ]
    rownames(Files) <- NULL
    F2 <- strsplit(Files$Name, "/")
    f1 <- 1
    f2 <- 1
    for (RO in F2) {
      if (length(RO) > Depth) {
        if (length(RO) >= Depth + 1) {
          # Check Depth Level
          if (Prev != RO[Depth + 1]) {
            if (length(RO) > Depth + 1) {
              FR <- data.frame(paste0("./", RO[Depth + 1]), "Directory",
                               Files[f1, 2:4], stringsAsFactors = FALSE)
              FR[, 4] <- "-"  # Second file name found so assumed blob was a directory

            } else FR <- data.frame(paste0("./", RO[Depth + 1]),
                                    "File", Files[f1, 2:4], stringsAsFactors = FALSE)

            colnames(FR)[2] <- "Type"

            FO <- rbind(FO, FR)

            f2 <- f2 + 1
          } else {
            FO[f2 - 1, 2] <- "Directory"  # Second file name found so assumed blob was a directory
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
        warning("Directory not found") else warning("No Files found")
      return(NULL)
    }
    AzureActiveContext$Directory <- DIR
    AzureActiveContext$Container <- CNTR
    AzureActiveContext$StorageAccount <- SAI
    AzureActiveContext$ResourceGroup <- RGI
    AzureActiveContext$DContainer <- CNTR

    rownames(FO) <- NULL
    colnames(FO)[1] <- "FileName"
    colnames(FO)[2] <- "Type"
    FN <- grep("Directory", FO$Type)  # Suffix / to Directory names
    FO$FileName[FN] <- paste0(FO$FileName[FN], "/")
    return(FO)
  }
}


#' Get contents from a specifed Storage Blob.
#'
#' @inheritParams SetAzureContext
#' @inheritParams AzureAuthenticate
#' @inheritParams AzureSAGetKey
#' @inheritParams AzureBlobLS

#' @param Type "Text" or "Raw"
#'
#' @family Blob store functions
#' @export

AzureGetBlob <- function(AzureActiveContext, Blob, Directory, Type = "text",
                         StorageAccount, StorageKey, Container, ResourceGroup, SubscriptionID,
                         AzToken, verbose = FALSE) {
  AzureCheckToken(AzureActiveContext)
  if (missing(SubscriptionID)) {
    SUBIDI <- AzureActiveContext$SubscriptionID
  } else (SUBIDI <- SubscriptionID)
  if (missing(AzToken)) {
    ATI <- AzureActiveContext$Token
  } else (ATI <- AzToken)
  if (missing(ResourceGroup)) {
    RGI <- AzureActiveContext$ResourceGroup
  } else (RGI <- ResourceGroup)
  if (missing(StorageAccount)) {
    SAI <- AzureActiveContext$StorageAccount
  } else (SAI <- StorageAccount)
  if (missing(StorageKey)) {
    STK <- AzureActiveContext$StorageKey
  } else (STK <- StorageKey)
  if (missing(Container)) {
    CNTR <- AzureActiveContext$Container
  } else (CNTR <- Container)
  if (missing(Blob)) {
    BLOBI <- AzureActiveContext$Blob
  } else (BLOBI <- Blob)
  verbosity <- if (verbose)
    httr::verbose(TRUE) else NULL

  if (length(RGI) < 1) {
    stop("Error: No ResourceGroup provided: Use ResourceGroup argument or set in AzureContext")
  }
  if (length(SAI) < 1) {
    stop("Error: No StorageAccount provided: Use StorageAccount argument or set in AzureContext")
  }
  if (length(CNTR) < 1) {
    stop("Error: No Container provided: Use Container argument or set in AzureContext")
  }
  if (length(BLOBI) < 1) {
    stop("Error: No Blob provided: Use Blob argument or set in AzureContext")
  }

  STK <- refreshStorageKey(AzureActiveContext)

  if (length(STK) < 1) {
    stop("Error: No StorageKey provided: Use StorageKey argument or set in AzureContext")
  }

  DIR <- AzureActiveContext$Directory
  DC <- AzureActiveContext$DContainer

  if (missing(Directory)) {
    if (length(DIR) < 1)
      DIR <- ""  # No previous Dir value
    if (length(DC) < 1) {
      DIR <- ""  # No previous Dir value
      DC <- ""
    } else if (CNTR != DC)
      DIR <- ""  # Change of Container
  } else DIR <- Directory

  if (nchar(DIR) > 0)
    DIR <- paste0(DIR, "/")

  BLOBI <- paste0(DIR, BLOBI)
  BLOBI <- gsub("^/", "", BLOBI)
  BLOBI <- gsub("^\\./", "", BLOBI)
  cat(BLOBI)

  URL <- paste("http://", SAI, ".blob.core.windows.net/", CNTR, "/",
               BLOBI, sep = "")


  D1 <- Sys.getlocale("LC_TIME")
  Sys.setlocale("LC_TIME", "C")
  # `x-ms-date` <- format(Sys.time(),'%a, %d %b %Y %H:%M:%S %Z',
  # tz='GMT')
  Sys.setlocale("LC_TIME", D1)
  D1 <- format(Sys.time(), "%a, %d %b %Y %H:%M:%S %Z", tz = "GMT")

  SIG <- GetSig(AzureActiveContext, URL, "GET", STK, SAI, Container = CNTR,
                CMD = paste0("/", BLOBI), DateS = D1)

  AT <- paste0("SharedKey ", SAI, ":", SIG)

  r <- GET(URL, add_headers(.headers = c(Authorization = AT, `Content-Length` = "0",
                                         `x-ms-version` = "2015-04-05", `x-ms-date` = D1)), verbosity)

  if (status_code(r) == 404) {
    cat(BLOBI)
    warning("File not found")
    return(NULL)
  } else if (status_code(r) != 200)
    stopWithAzureError(r)

  r2 <- content(r, Type, encoding = "UTF-8")

  AzureActiveContext$StorageAccount <- SAI
  AzureActiveContext$ResourceGroup <- RGI
  AzureActiveContext$StorageKey <- STK
  AzureActiveContext$Container <- CNTR
  AzureActiveContext$Blob <- BLOBI
  return(r2)
}


#' Write contents to a specifed Storage Blob.
#'
#' @inheritParams SetAzureContext
#' @inheritParams AzureAuthenticate
#' @inheritParams AzureSAGetKey
#' @inheritParams AzureBlobLS
#'
#' @param Contents - Object to Store or Value
#' @param File - Local FileName to Store in Azure Blob
#'
#' @family Blob store functions
#' @export
AzurePutBlob <- function(AzureActiveContext, Blob, Contents = "", File = "",
                         Directory, StorageAccount, StorageKey,
                         Container, ResourceGroup, SubscriptionID,
                         AzToken, verbose = FALSE) {

  AzureCheckToken(AzureActiveContext)

  if (missing(SubscriptionID)) {
    SUBIDI <- AzureActiveContext$SubscriptionID
  } else (SUBIDI <- SubscriptionID)
  if (missing(AzToken)) {
    ATI <- AzureActiveContext$Token
  } else (ATI <- AzToken)
  if (missing(ResourceGroup)) {
    RGI <- AzureActiveContext$ResourceGroup
  } else (RGI <- ResourceGroup)
  if (missing(StorageAccount)) {
    SAI <- AzureActiveContext$StorageAccount
  } else (SAI <- StorageAccount)
  if (missing(StorageKey)) {
    STK <- AzureActiveContext$StorageKey
  } else (STK <- StorageKey)
  if (missing(Container)) {
    CNTR <- AzureActiveContext$Container
  } else (CNTR <- Container)
  if (missing(Blob)) {
    BLOBI <- AzureActiveContext$Blob
  } else (BLOBI <- Blob)
  verbosity <- if (verbose)
    httr::verbose(TRUE) else NULL

  if (length(RGI) < 1) {
    stop("Error: No ResourceGroup provided: Use ResourceGroup argument or set in AzureContext")
  }
  if (length(SAI) < 1) {
    stop("Error: No StorageAccount provided: Use StorageAccount argument or set in AzureContext")
  }
  if (length(CNTR) < 1) {
    stop("Error: No Container provided: Use Container argument or set in AzureContext")
  }
  if (length(BLOBI) < 1) {
    stop("Error: No Blob provided: Use Blob argument or set in AzureContext")
  }

  DIR <- AzureActiveContext$Directory
  DC <- AzureActiveContext$DContainer

  if (missing(Directory)) {

    if (length(DIR) < 1)
      DIR <- ""  # No previous Dir value
    if (length(DC) < 1) {
      DIR <- ""  # No previous Dir value
      DC <- ""
    } else if (CNTR != DC)
      DIR <- ""  # Change of Container
  } else DIR <- Directory
  if (nchar(DIR) > 0)
    DIR <- paste0(DIR, "/")

  BLOBI <- paste0(DIR, BLOBI)
  BLOBI <- gsub("^/", "", BLOBI)
  BLOBI <- gsub("//", "/", BLOBI)
  BLOBI <- gsub("//", "/", BLOBI)

  if (missing(Contents) && missing(File))
    stop("Content or File needs to be supplied")

  if (!missing(Contents) && !missing(File))
    stop("Provided either Content OR File Argument")


  STK <- refreshStorageKey(AzureActiveContext)

  if (length(STK) < 1) {
    stop("Error: No StorageKey provided: Use StorageKey argument or set in AzureContext")
  }

  URL <- paste("http://", SAI, ".blob.core.windows.net/", CNTR, "/",
               BLOBI, sep = "")

  D1 <- Sys.getlocale("LC_TIME")
  Sys.setlocale("LC_TIME", "C")
  Sys.setlocale("LC_TIME", D1)
  D1 <- format(Sys.time(), "%a, %d %b %Y %H:%M:%S %Z", tz = "GMT")
  if (nchar(Contents) == 0)
    Contents <- "-"

  SIG <- GetSig(AzureActiveContext, URL, "PUT", STK, SAI, ContentType = "text/plain; charset=UTF-8",
                Size = nchar(Contents), Headers = "x-ms-blob-type:BlockBlob", Container = CNTR,
                CMD = paste0("/", BLOBI), DateS = D1)

  AT <- paste0("SharedKey ", SAI, ":", SIG)

  r <- PUT(URL, add_headers(.headers = c(Authorization = AT,
                                         `Content-Length` = nchar(Contents),
                                         `x-ms-version` = "2015-04-05",
                                         `x-ms-date` = D1,
                                         `x-ms-blob-type` = "BlockBlob",
                                         `Content-Type` = "text/plain; charset=UTF-8")),
           body = Contents,
           verbosity)

  AzureActiveContext$Blob <- BLOBI
  return(paste("Blob:", BLOBI, " Saved:", nchar(Contents), "bytes written"))
}




#' Find File in a Storage account directory.
#'
#' @inheritParams SetAzureContext
#' @inheritParams AzureAuthenticate
#' @inheritParams AzureSAGetKey
#' @inheritParams AzurePutBlob
#'
#' @family Blob store functions
#' @export
AzureBlobFind <- function(AzureActiveContext, File, StorageAccount, StorageKey,
                          Container, ResourceGroup, SubscriptionID,
                          AzToken, verbose = FALSE) {
  AzureCheckToken(AzureActiveContext)
  if (missing(SubscriptionID)) {
    SUBIDI <- AzureActiveContext$SubscriptionID
  } else (SUBIDI <- SubscriptionID)
  if (missing(AzToken)) {
    ATI <- AzureActiveContext$Token
  } else (ATI <- AzToken)
  if (missing(ResourceGroup)) {
    RGI <- AzureActiveContext$ResourceGroup
  } else (RGI <- ResourceGroup)
  if (missing(StorageAccount)) {
    SAI <- AzureActiveContext$StorageAccount
  } else (SAI <- StorageAccount)
  if (missing(StorageKey)) {
    STK <- AzureActiveContext$StorageKey
  } else (STK <- StorageKey)
  if (missing(File)) {
    stop("Error: No Filename{pattern} provided")
  }
  verbosity <- if (verbose)
    httr::verbose(TRUE) else NULL

  if (length(RGI) < 1) {
    stop("Error: No ResourceGroup provided: Use ResourceGroup argument or set in AzureContext")
  }
  if (length(SAI) < 1) {
    stop("Error: No StorageAccount provided: Use StorageAccount argument or set in AzureContext")
  }


  STK <- refreshStorageKey(AzureActiveContext)

  if (length(STK) < 1) {
    stop("Error: No StorageKey provided: Use StorageKey argument or set in AzureContext")
  }

  if (!missing(Container)) {
    CL <- Container
  } else {
    CL <- AzureListSAContainers(AzureActiveContext)$Name
  }

  F2 <- data.frame()
  for (CI in CL) {
    Files <- AzureListSABlobs(AzureActiveContext, Container = CI)
    Files$Name <- paste0("/", Files$Name)
    F1 <- grep(File, Files$Name)
    Files <- Files[F1, 1:4]
    Files <- cbind(Container = CI, Files)
    F2 <- rbind(F2, Files)
  }
  rownames(F2) <- NULL
  return(F2)
}


#' Azure Blob change current Directory.
#'
#' @inheritParams SetAzureContext
#' @inheritParams AzureAuthenticate
#' @inheritParams AzureSAGetKey
#' @inheritParams AzureBlobLS
#' @inheritParams AzurePutBlob
#'
#' @family Blob store functions
#' @export
AzureBlobCD <- function(AzureActiveContext, Directory, Container, File,
                        StorageAccount, StorageKey, ResourceGroup, SubscriptionID,
                        AzToken,verbose = FALSE) {
  AzureCheckToken(AzureActiveContext)
  if (missing(SubscriptionID)) {
    SUBIDI <- AzureActiveContext$SubscriptionID
  } else (SUBIDI <- SubscriptionID)
  if (missing(AzToken)) {
    ATI <- AzureActiveContext$Token
  } else (ATI <- AzToken)
  if (missing(ResourceGroup)) {
    RGI <- AzureActiveContext$ResourceGroup
  } else (RGI <- ResourceGroup)
  if (missing(StorageAccount)) {
    SAI <- AzureActiveContext$StorageAccount
  } else (SAI <- StorageAccount)
  if (missing(StorageKey)) {
    STK <- AzureActiveContext$StorageKey
  } else (STK <- StorageKey)
  if (missing(Container)) {
    CNTR <- AzureActiveContext$Container
  } else (CNTR <- Container)
  verbosity <- if (verbose)
    httr::verbose(TRUE) else NULL

  if (length(RGI) < 1) {
    stop("Error: No ResourceGroup provided: Use ResourceGroup argument or set in AzureContext")
  }
  if (length(SAI) < 1) {
    stop("Error: No StorageAccount provided: Use StorageAccount argument or set in AzureContext")
  }

  if (missing(Directory)) {
    DIR <- AzureActiveContext$Directory
    DC <- AzureActiveContext$DContainer
    if (length(DIR) < 1)
      DIR <- "/"  # No previous Dir value
    if (length(DC) < 1) {
      DIR <- "/"  # No previous Dir value
      DC <- ""
    } else if (CNTR != DC)
      DIR <- "/"  # Change of Container

    AzureActiveContext$Directory <- DIR
    AzureActiveContext$Container <- CNTR
    AzureActiveContext$StorageAccount <- SAI
    AzureActiveContext$ResourceGroup <- RGI
    AzureActiveContext$DContainer <- CNTR
    return(paste0("Current Directory - ", SAI, " >  ", CNTR, " : ",
                  DIR))

  }
  STK <- refreshStorageKey(AzureActiveContext)
  if (length(STK) < 1) {
    stop("Error: No StorageKey provided: Use StorageKey argument or set in AzureContext")
  }

  if (Directory == "../" || Directory == "..") {
    # Basic attempt at relative paths
    Directory <- gsub("/[a-zA-Z0-9]*$", "", AzureActiveContext$Directory)
  }

  if (Directory == "../..") {
    Directory <- gsub("/[a-zA-Z0-9]*$", "", AzureActiveContext$Directory)
    Directory <- gsub("/[a-zA-Z0-9]*$", "", Directory)
  }

  if (Directory == "../../..") {
    Directory <- gsub("/[a-zA-Z0-9]*$", "", AzureActiveContext$Directory)
    Directory <- gsub("/[a-zA-Z0-9]*$", "", Directory)
    Directory <- gsub("/[a-zA-Z0-9]*$", "", Directory)
  }

  AzureActiveContext$Directory <- Directory
  AzureActiveContext$Container <- CNTR
  AzureActiveContext$DContainer <- CNTR
  AzureActiveContext$StorageAccount <- SAI
  AzureActiveContext$ResourceGroup <- RGI
  AzureActiveContext$DContainer <- CNTR

  return(paste0("Current Directory - ", SAI, " >  ", CNTR, " : ", Directory))
}


