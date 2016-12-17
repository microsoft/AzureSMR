#' List Storage Containers for Specified Storage Account.
#'
#' @inheritParams SetAzureContext
#' @inheritParams AzureAuthenticate
#' @inheritParams AzureSAGetKey

#' @family Storage account
# @param AzureActiveContext Azure Context Object
# @param StorageAccount StorageAccount
# @param StorageKey StorageKey
# @param ResourceGroup ResourceGroup Object (or use AzureActiveContext)
# @param Token Token Object (or use AzureActiveContext)
# @param SubscriptionID SubscriptionID Object (or use AzureActiveContext)
# @param verbose Print Tracing information (Default False)
#'
#' @export
AzureListSAContainers <- function(AzureActiveContext,StorageAccount,StorageKey,ResourceGroup,AzToken,SubscriptionID,verbose = FALSE) {
  AzureCheckToken(AzureActiveContext)

  if(missing(SubscriptionID)) {SUBIDI <- AzureActiveContext$SubscriptionID} else (SUBIDI = SubscriptionID)
  if(missing(AzToken)) {ATI <- AzureActiveContext$Token} else (ATI = AzToken)
  if(missing(ResourceGroup)) {RGI <- AzureActiveContext$ResourceGroup} else (RGI = ResourceGroup)
  if(missing(StorageAccount)) {SAI <- AzureActiveContext$StorageAccount} else (SAI = StorageAccount)
  verbosity <- if(verbose) httr::verbose(TRUE) else NULL

  if (length(RGI)<1) {stop("Error: No ResourceGroup provided: Use ResourceGroup argument or set in AzureContext")}
  if (length(SAI)<1) {stop("Error: No StorageAccount provided: Use StorageAccount argument or set in AzureContext")}

  if (length(AzureActiveContext$StorageAccountK) <1 || SAI != AzureActiveContext$StorageAccountK|| length(AzureActiveContext$StorageKey) <1)
  {
    print("Fetching Storage Key..")
    STK <- AzureSAGetKey(AzureActiveContext, ResourceGroup = RGI, StorageAccount = SAI)
  }
  else
    STK <- AzureActiveContext$StorageKey


  if (length(STK)<1) {stop("Error: No StorageKey provided: Use StorageKey argument or set in AzureContext")}

  URL <- paste("http://",SAI,".blob.core.windows.net/?comp=list",sep="")

  #  r<-OLDazureBlobCall(AzureActiveContext,URL, "GET", key=STK)

  D1 <- Sys.getlocale("LC_TIME"); Sys.setlocale("LC_TIME", "C")
  `x-ms-date` <- format(Sys.time(),"%a, %d %b %Y %H:%M:%S %Z", tz="GMT")
  Sys.setlocale("LC_TIME", D1)
  D1 <- format(Sys.time(),"%a, %d %b %Y %H:%M:%S %Z", tz="GMT")

  SIG <- GetSig(AzureActiveContext,URL,"GET",STK,SAI,CMD="\ncomp:list",DateS=D1)

  AT <- paste0("SharedKey ", SAI, ":", SIG)

  r <- GET(URL,add_headers(.headers = c("Authorization" = AT, "Content-Length" = "0","x-ms-version"="2015-04-05","x-ms-date"=D1)),verbosity)

  if (status_code(r) != 200) stop(paste0("Error: Return code(",status_code(r),")" ))
  r <- content(r,"text",encoding="UTF-8")

  y<-htmlParse(r)
  namesx <- xpathApply(y, "//containers//container/name",xmlValue)

  if (length(namesx) == 0) {
    warning("Storage accounts have no ");
    return(NULL)
  }

  lmodx <- xpathApply(y, "//containers//container/properties/last-modified",xmlValue)
  statusx <- xpathApply(y, "//containers//container/properties/leasestatus",xmlValue)
  statex <- xpathApply(y, "//containers//container/properties/leasestate",xmlValue)
  etag <- xpathApply(y, "//containers//container/properties/etag",xmlValue)

  if (length(namesx) == 0 )
  {
    warning("No containers found in Storage account")
    return(NULL)
  }

  dfn <- as.data.frame(matrix(ncol=5,nrow=length(namesx)))

  for(i in 1:length(namesx))
  {
    dfn[i,1] <- namesx[i]
    dfn[i,2] <- lmodx[i]
    dfn[i,3] <- statusx[i]
    dfn[i,4] <- statex[i]
    dfn[i,5] <- etag[i]
  }
  colnames(dfn) <- c("Name", "Last-Modified", "Status", "State", "Etag")
  AzureActiveContext$StorageAccount <- SAI
  AzureActiveContext$ResourceGroup <- RGI
  AzureActiveContext$StorageKey <- STK
  return(dfn)
}


#' Create Storage Containers in a specified Storage Account.
#'
#' @inheritParams SetAzureContext
#' @inheritParams AzureAuthenticate
#' @inheritParams AzureSAGetKey

#' @family Storage account
# @param AzureActiveContext Azure Context Object
# @param Container - Storage Container
# @param StorageAccount StorageAccount
# @param StorageKey StorageKey
# @param ResourceGroup ResourceGroup Object (or use AzureActiveContext)
# @param Token Token Object (or use AzureActiveContext)
# @param SubscriptionID SubscriptionID Object (or use AzureActiveContext)
# @param verbose Print Tracing information (Default False)
#'
#' @export
AzureCreateSAContainer <- function(AzureActiveContext,Container,StorageAccount,StorageKey,ResourceGroup,AzToken,SubscriptionID,verbose = FALSE) {
  #  AzureCheckToken(AzureActiveContext)

  if(missing(SubscriptionID)) {SUBIDI <- AzureActiveContext$SubscriptionID} else (SUBIDI = SubscriptionID)
  if(missing(AzToken)) {ATI <- AzureActiveContext$Token} else (ATI = AzToken)
  if(missing(ResourceGroup)) {RGI <- AzureActiveContext$ResourceGroup} else (RGI = ResourceGroup)
  if(missing(StorageAccount)) {SAI <- AzureActiveContext$StorageAccount} else (SAI = StorageAccount)
  if(missing(StorageKey)) {STK <- AzureActiveContext$StorageKey} else (STK = StorageKey)
  if(missing(Container)) {stop("Error: No Container name provided")}
  verbosity <- if(verbose) httr::verbose(TRUE) else NULL

  if (length(RGI)<1) {stop("Error: No ResourceGroup provided: Use ResourceGroup argument or set in AzureContext")}
  if (length(SAI)<1) {stop("Error: No StorageAccount provided: Use StorageAccount argument or set in AzureContext")}

  if (length(AzureActiveContext$StorageAccountK) <1 || SAI != AzureActiveContext$StorageAccountK|| length(AzureActiveContext$StorageKey) <1)
  {
    cat("Fetching Storage Key..")
    STK <- AzureSAGetKey(AzureActiveContext, ResourceGroup = RGI, StorageAccount = SAI)
  }
  else
    STK <- AzureActiveContext$StorageKey

  if (length(STK)<1) {stop("Error: No StorageKey provided: Use StorageKey argument or set in AzureContext")}

  URL <- paste("https://",SAI,".blob.core.windows.net//",Container,"?restype=container",sep="")


  #  r<-OLDazureBlobCall(AzureActiveContext,URL, "GET", key=STK)

  D1 <- Sys.getlocale("LC_TIME"); Sys.setlocale("LC_TIME", "C")
  `x-ms-date` <- format(Sys.time(),"%a, %d %b %Y %H:%M:%S %Z", tz="GMT")
  Sys.setlocale("LC_TIME", D1)
  D1 <- format(Sys.time(),"%a, %d %b %Y %H:%M:%S %Z", tz="GMT")
  CNTR = Container

  AzureActiveContext$Container <- Container
  AzureActiveContext$StorageAccount = SAI
  AzureActiveContext$ResourceGroup = RGI
  #  SIG <- GetSig(AzureActiveContext,URL,"GET",STK,StorageAccount,CMD="\nrestype:container",DateS=D1)

  URL <- paste("http://",SAI,".blob.core.windows.net/",Container,"?restype=container",sep="")

  D1 <- Sys.getlocale("LC_TIME"); Sys.setlocale("LC_TIME", "us")
  #    `x-ms-date` <- format(Sys.time(),"%a, %d %b %Y %H:%M:%S %Z", tz="GMT")
  Sys.setlocale("LC_TIME", D1)
  D1 <- format(Sys.time(),"%a, %d %b %Y %H:%M:%S %Z", tz="GMT")

  #  SIG <- GetSig(AzureActiveContext,URL,"PUT",STK,StorageAccount,Container=CNTR,DateS=D1)
  SIG <- GetSig(AzureActiveContext,URL,"PUT",STK,SAI,Container=CNTR,CMD="\nrestype:container",DateS=D1)

  AT <- paste0("SharedKey ", SAI, ":", SIG)
  r <- PUT(URL,add_headers(.headers = c("Authorization" = AT, "Content-Length" = "0","x-ms-version"="2015-04-05","x-ms-date"=D1)),verbosity)

  if (status_code(r) == 201) {return("Container created OK")}
  stop(paste0("Error: Return code(",status_code(r),")" ))
  return("OK")
}


#' Delete Storage Container in a specified Storage Account.
#'
#' @inheritParams SetAzureContext
#' @inheritParams AzureAuthenticate
#' @inheritParams AzureSAGetKey

#' @family Storage account
# @param AzureActiveContext Azure Context Object
# @param Container - Storage Container
# @param StorageAccount StorageAccount
# @param StorageKey StorageKey
# @param ResourceGroup ResourceGroup Object (or use AzureActiveContext)
# @param Token Token Object (or use AzureActiveContext)
# @param SubscriptionID SubscriptionID Object (or use AzureActiveContext)
# @param verbose Print Tracing information (Default False)
#'
#' @export
AzureDeleteSAContainer <- function(AzureActiveContext,Container,StorageAccount,StorageKey,ResourceGroup,AzToken,SubscriptionID,verbose = FALSE) {
  AzureCheckToken(AzureActiveContext)

  if(missing(SubscriptionID)) {SUBIDI <- AzureActiveContext$SubscriptionID} else (SUBIDI = SubscriptionID)
  if(missing(AzToken)) {ATI <- AzureActiveContext$Token} else (ATI = AzToken)
  if(missing(ResourceGroup)) {RGI <- AzureActiveContext$ResourceGroup} else (RGI = ResourceGroup)
  if(missing(StorageAccount)) {SAI <- AzureActiveContext$StorageAccount} else (SAI = StorageAccount)
  if(missing(StorageKey)) {STK <- AzureActiveContext$StorageKey} else (STK = StorageKey)
  if(missing(Container)) {stop("Error: No Container name provided")}
  verbosity <- if(verbose) httr::verbose(TRUE) else NULL

  CNTR = Container

  if (length(RGI)<1) {stop("Error: No ResourceGroup provided: Use ResourceGroup argument or set in AzureContext")}
  if (length(SAI)<1) {stop("Error: No StorageAccount provided: Use StorageAccount argument or set in AzureContext")}

  if (length(AzureActiveContext$StorageAccountK) <1 || SAI != AzureActiveContext$StorageAccountK)
  {
    print("Fetching Storage Key..")
    STK <- AzureSAGetKey(AzureActiveContext, ResourceGroup = RGI, StorageAccount = SAI)
  }
  if (length(STK)<1) {stop("Error: No StorageKey provided: Use StorageKey argument or set in AzureContext")}


  URL <- paste("http://",SAI,".blob.core.windows.net/",Container,"?restype=container",sep="")

  #  r<-OLDazureBlobCall(AzureActiveContext,URL, "GET", key=STK)

  D1 <- Sys.getlocale("LC_TIME"); Sys.setlocale("LC_TIME", "C")
  `x-ms-date` <- format(Sys.time(),"%a, %d %b %Y %H:%M:%S %Z", tz="GMT")
  Sys.setlocale("LC_TIME", D1)
  D1 <- format(Sys.time(),"%a, %d %b %Y %H:%M:%S %Z", tz="GMT")
  CNTR = Container

  AzureActiveContext$Container <- CNTR
  AzureActiveContext$StorageAccount = SAI
  AzureActiveContext$ResourceGroup = RGI
  SIG <- GetSig(AzureActiveContext,URL,"DELETE",STK,SAI,CMD=paste0(CNTR,"\nrestype:container"),DateS=D1)

  AT <- paste0("SharedKey ", SAI, ":", SIG)

  r <- DELETE(URL,add_headers(.headers = c("Authorization" = AT, "Content-Length" = "0","x-ms-version"="2015-04-05","x-ms-date"=D1)),verbosity)

  if (status_code(r) == 202) {return("Container delete request accepted")}
  stop(paste0("Error: Return code(",status_code(r),")" ))
  return("OK")
}

