GetSig <- function(AzureActiveContext,url, verb, key, StorageAccount,Headers=NULL,Container=NULL,CMD=NULL,Size=NULL,ContentType=NULL,DateS,verbose = FALSE) {

  if (length(Headers))
    ARG1 <- paste0(Headers,"\nx-ms-date:",DateS,"\nx-ms-version:2015-04-05")
  else
    ARG1 <- paste0("x-ms-date:",DateS,"\nx-ms-version:2015-04-05")

  ARG2 <- paste0("/",StorageAccount,"/",Container, CMD)

  SIG <- paste0(verb, "\n\n\n", Size, "\n\n", ContentType, "\n\n\n\n\n\n\n", ARG1, "\n", ARG2)

  if (verbose) print (paste0("TRACE: STRINGTOSIGN: ",SIG))

  EKEY <- base64encode(hmac(key=RCurl::base64Decode(key, mode="raw"),object=iconv(SIG, "ASCII", to = "UTF-8"),algo="sha256",raw=TRUE))
  return (EKEY);
}

#' @name AzureSM: AzureSAGetKey
#' @title Get the Storage Keys for Specified Storage Account
#' @param AzureActiveContext Azure Context Object
#' @param StorageAccount StorageAccount
#' @param Token Token Object (or use AzureActiveContext)
#' @param ResourceGroup ResourceGroup Object (or use AzureActiveContext)
#' @param Verbose Print Tracing information (Default False)
#' @rdname AzureSAGetKey
#' @export
AzureSAGetKey <- function(AzureActiveContext,StorageAccount,AzToken,ResourceGroup,SubscriptionID,verbose = FALSE) {
  if(missing(ResourceGroup)) {RGI <- AzureActiveContext$ResourceGroup} else (RGI = ResourceGroup)
  if(missing(AzToken)) {ATI <- AzureActiveContext$Token} else (ATI = AzToken)

  if(missing(SubscriptionID)) {SUBIDI <- AzureActiveContext$SubscriptionID} else (SUBIDI = SubscriptionID)
  if (!length(StorageAccount)) {stop("Error: No Valid StorageAccount provided")}
  if (length(RGI)<1) {stop("Error: No ResourceGroup provided: Use ResourceGroup argument or set in AzureContext")}
  if (!length(ATI)) {stop("Error: No Token / Not currently Authenticated.")}
  if (!length(SUBIDI)) {stop("Error: No SubscriptionID provided: Use SUBID argument or set in AzureContext")}
  verbosity <- if(verbose) httr::verbose(TRUE) else NULL

  URL <- paste("https://management.azure.com/subscriptions/",SUBIDI,"/resourceGroups/",RGI,"/providers/Microsoft.Storage/storageAccounts/",StorageAccount,"/listkeys?api-version=2016-01-01",sep="")

  r <- POST(URL,add_headers(.headers = c("Host" = "management.azure.com" ,"Authorization" = ATI, "Content-Type" = "application/json")),verbosity)


  rl <- content(r,"text",encoding="UTF-8")
  df <- fromJSON(rl)
  AzureActiveContext$StorageAccount <- StorageAccount
  AzureActiveContext$ResourceGroup <- ResourceGroup
  AzureActiveContext$StorageKey <- df$keys$value[1]
  return(AzureActiveContext$StorageKey)
}

#' @name AzureSM: AzureListSAContainers
#' @title List Storage Containers for Specified Storage Account
#' @param AzureActiveContext Azure Context Object
#' @param StorageAccount StorageAccount
#' @param StorageKey StorageKey
#' @param ResourceGroup ResourceGroup Object (or use AzureActiveContext)
#' @param Token Token Object (or use AzureActiveContext)
#' @param SubscriptionID SubscriptionID Object (or use AzureActiveContext)
#' @param Verbose Print Tracing information (Default False)
#' @rdname AzureListSAContainers
#' @export
AzureListSAContainers <- function(AzureActiveContext,StorageAccount,StorageKey,ResourceGroup,AzToken,SubscriptionID,verbose = FALSE) {

  if(missing(SubscriptionID)) {SUBIDI <- AzureActiveContext$SubscriptionID} else (SUBIDI = SubscriptionID)
  if(missing(AzToken)) {ATI <- AzureActiveContext$Token} else (ATI = AzToken)
  if(missing(ResourceGroup)) {RGI <- AzureActiveContext$ResourceGroup} else (RGI = ResourceGroup)
  if(missing(StorageAccount)) {SAI <- AzureActiveContext$StorageAccount} else (SAI = StorageAccount)
  if(missing(StorageKey)) {STK <- AzureActiveContext$StorageKey} else (STK = StorageKey)
  verbosity <- if(verbose) httr::verbose(TRUE) else NULL

  if (length(RGI)<1) {stop("Error: No ResourceGroup provided: Use ResourceGroup argument or set in AzureContext")}
  if (length(SAI)<1) {stop("Error: No StorageAccount provided: Use StorageAccount argument or set in AzureContext")}
  if (length(STK)<1) {stop("Error: No StorageKey provided: Use StorageKey argument or set in AzureContext")}

  URL <- paste("http://",SAI,".blob.core.windows.net/?comp=list",sep="")

#  r<-OLDazureBlobCall(AzureActiveContext,URL, "GET", key=STK)

  D1 <- Sys.getlocale("LC_TIME"); Sys.setlocale("LC_TIME", "us")
  `x-ms-date` <- format(Sys.time(),"%a, %d %b %Y %H:%M:%S %Z", tz="GMT")
  Sys.setlocale("LC_TIME", D1)
  D1 <- format(Sys.time(),"%a, %d %b %Y %H:%M:%S %Z", tz="GMT")

  SIG <- GetSig(AzureActiveContext,URL,"GET",STK,StorageAccount,CMD="\ncomp:list",DateS=D1)

  AT <- paste0("SharedKey ", StorageAccount, ":", SIG)

    r <- GET(URL,add_headers(.headers = c("Authorization" = AT, "Content-Length" = "0","x-ms-version"="2015-04-05","x-ms-date"=D1)),verbosity)

  r <- content(r,"text",encoding="UTF-8")
  y<-htmlParse(r)

  namesx <- xpathApply(y, "//containers//container/name",xmlValue)
  lmodx <- xpathApply(y, "//containers//container/properties/last-modified",xmlValue)
  statusx <- xpathApply(y, "//containers//container/properties/leasestatus",xmlValue)
  statex <- xpathApply(y, "//containers//container/properties/leasestate",xmlValue)
  etag <- xpathApply(y, "//containers//container/properties/etag",xmlValue)

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
#' @name AzureSM: AzureListSABlobs
#' @title List Storage Containers for Specified Storage Account
#' @param AzureActiveContext Azure Context Object
#' @param StorageAccount StorageAccount
#' @param StorageKey StorageKey
#' @param Container Storage Container
#' @param ResourceGroup ResourceGroup Object (or use AzureActiveContext)
#' @param SubscriptionID SubscriptionID Object (or use AzureActiveContext)
#' @param AzToken Token Object (or use AzureActiveContext)
#' @param Verbose Print Tracing information (Default False)
#' @rdname AzureListSABlobs
#' @export
AzureListSABlobs <- function(AzureActiveContext,StorageAccount,StorageKey,Container,ResourceGroup,SubscriptionID,AzToken) {

  if(missing(SubscriptionID)) {SUBIDI <- AzureActiveContext$SubscriptionID} else (SUBIDI = SubscriptionID)
  if(missing(AzToken)) {ATI <- AzureActiveContext$Token} else (ATI = AzToken)
  if(missing(ResourceGroup)) {RGI <- AzureActiveContext$ResourceGroup} else (RGI = ResourceGroup)
  if(missing(StorageAccount)) {SAI <- AzureActiveContext$StorageAccount} else (SAI = StorageAccount)
  if(missing(StorageKey)) {STK <- AzureActiveContext$StorageKey} else (STK = StorageKey)
  if(missing(Container)) {CNTR <- AzureActiveContext$Container} else (CNTR = Container)
  verbosity <- if(verbose) httr::verbose(TRUE) else NULL

  if (length(RGI)<1) {stop("Error: No ResourceGroup provided: Use ResourceGroup argument or set in AzureContext")}
  if (length(SAI)<1) {stop("Error: No StorageAccount provided: Use StorageAccount argument or set in AzureContext")}
  if (length(STK)<1) {stop("Error: No StorageKey provided: Use StorageKey argument or set in AzureContext")}
  if (length(CNTR)<1) {stop("Error: No Container provided: Use Container argument or set in AzureContext")}


  URL <- paste("http://",SAI,".blob.core.windows.net/",CNTR,"?restype=container&comp=list",sep="")

  D1 <- Sys.getlocale("LC_TIME"); Sys.setlocale("LC_TIME", "us")
#    `x-ms-date` <- format(Sys.time(),"%a, %d %b %Y %H:%M:%S %Z", tz="GMT")
  Sys.setlocale("LC_TIME", D1)
  D1 <- format(Sys.time(),"%a, %d %b %Y %H:%M:%S %Z", tz="GMT")

  SIG <- GetSig(AzureActiveContext,URL,"GET",STK,StorageAccount,Container=CNTR,CMD="\ncomp:list\nrestype:container",DateS=D1)

  AT <- paste0("SharedKey ", StorageAccount, ":", SIG)
    #  GetSig <- function(AzureActiveContext,url, verb, key, StorageAccount,Headers=NULL,Container=NULL,CMD=NULL,Size=NULL) {
  r <- GET(URL,add_headers(.headers = c("Authorization" = AT, "Content-Length" = "0","x-ms-version"="2015-04-05","x-ms-date"=D1)),verbosity)

  y<-htmlParse(r)

  namesx <- xpathApply(y, "//blobs//blob//name",xmlValue)
  lmodx <- xpathApply(y, "//blobs//blob//properties/last-modified",xmlValue)
  lenx <- xpathApply(y, "//blobs//blob//properties/content-length",xmlValue)
  bltx <- xpathApply(y, "//blobs//blob//properties/blobtype",xmlValue)
  cpx <- xpathApply(y, "//blobs//blob//properties/leasestate",xmlValue)

  dfn <- as.data.frame(matrix(ncol=5,nrow=length(namesx)))

  for(i in 1:length(namesx))
  {
    dfn[i,1] <- namesx[i]
    dfn[i,2] <- lmodx[i]
    dfn[i,3] <- lenx[i]
    dfn[i,4] <- bltx[i]
    dfn[i,5] <- cpx[i]
  }
  colnames(dfn) <- c("Name", "LastModified", "Length", "Type", "LeaseState")
  AzureActiveContext$StorageAccount <- SAI
  AzureActiveContext$ResourceGroup <- RGI
  AzureActiveContext$StorageKey <- STK
  return(dfn)
}

#' @name AzureSM: AzureGetBlob
#' @title Get contents from a specifed Storage Blob
#' @param AzureActiveContext Azure Context Object
#' @param StorageAccount StorageAccount
#' @param StorageKey StorageKey
#' @param Container Storage Container
#' @param Blob Blob Name
#' @param Blob Type ("Text" or "Raw")
#' @param ResourceGroup ResourceGroup Object (or use AzureActiveContext)
#' @param Token Token Object (or use AzureActiveContext)
#' @param SubscriptionID SubscriptionID Object (or use AzureActiveContext)
#' @param Verbose Print Tracing information (Default False)
#' @rdname AzureGetBlob
#' @export
#' @importFrom digest hmac
#' @importFrom base64enc base64encode base64decode

AzureGetBlob <- function(AzureActiveContext,StorageAccount,StorageKey,Container,Blob, Type="Text",ResourceGroup,SubscriptionID,AzToken,Verbose=TRUE) {

  if(missing(SubscriptionID)) {SUBIDI <- AzureActiveContext$SubscriptionID} else (SUBIDI = SubscriptionID)
  if(missing(AzToken)) {ATI <- AzureActiveContext$Token} else (ATI = AzToken)
  if(missing(ResourceGroup)) {RGI <- AzureActiveContext$ResourceGroup} else (RGI = ResourceGroup)
  if(missing(StorageAccount)) {SAI <- AzureActiveContext$StorageAccount} else (SAI = StorageAccount)
  if(missing(StorageKey)) {STK <- AzureActiveContext$StorageKey} else (STK = StorageKey)
  if(missing(Container)) {CNTR <- AzureActiveContext$Container} else (CNTR = Container)
  if(missing(Blob)) {BLOBI <- AzureActiveContext$Blob} else (BLOBI = Blob)
  verbosity <- if(verbose) httr::verbose(TRUE) else NULL

  if (length(RGI)<1) {stop("Error: No ResourceGroup provided: Use ResourceGroup argument or set in AzureContext")}
  if (length(SAI)<1) {stop("Error: No StorageAccount provided: Use StorageAccount argument or set in AzureContext")}
  if (length(STK)<1) {stop("Error: No StorageKey provided: Use StorageKey argument or set in AzureContext")}
  if (length(CNTR)<1) {stop("Error: No Container provided: Use Container argument or set in AzureContext")}
  if (length(BLOBI)<1) {stop("Error: No Blob provided: Use Blob argument or set in AzureContext")}

  URL <- paste("http://",SAI,".blob.core.windows.net/",CNTR,"/",BLOBI,sep="")

  D1 <- Sys.getlocale("LC_TIME"); Sys.setlocale("LC_TIME", "us")
  #`x-ms-date` <- format(Sys.time(),"%a, %d %b %Y %H:%M:%S %Z", tz="GMT")
  Sys.setlocale("LC_TIME", D1)
  D1 <- format(Sys.time(),"%a, %d %b %Y %H:%M:%S %Z", tz="GMT")

  SIG <- GetSig(AzureActiveContext,URL,"GET",STK,StorageAccount,Container=CNTR,CMD=paste0("/",BLOBI),DateS=D1)

  AT <- paste0("SharedKey ", StorageAccount, ":", SIG)

  r <- GET(URL,add_headers(.headers = c("Authorization" = AT, "Content-Length" = "0","x-ms-version"="2015-04-05","x-ms-date"=D1)),verbosity)

  r2 <- content(r,Type,encoding="UTF-8")

  AzureActiveContext$StorageAccount <- SAI
  AzureActiveContext$ResourceGroup <- RGI
  AzureActiveContext$StorageKey <- STK
  AzureActiveContext$Container <- CNTR
  AzureActiveContext$Blob <- BLOBI
  return(r2)
}

#' @name AzureSM: AzurePutBlob
#' @title Write contents to a specifed Storage Blob
#' @param AzureActiveContext -Azure Context Object
#' @param StorageAccount - StorageAccount
#' @param StorageKey - StorageKey
#' @param Container - Storage Container
#' @param Blob - Blob Name
#' @param ResourceGroup ResourceGroup Object (or use AzureActiveContext)
#' @param Contents - Object to Store or Value
#' @param File - Local FileName to Store in Azure Blob
#' @param Token - Token Object (or use AzureActiveContext)
#' @param SubscriptionID - SubscriptionID Object (or use AzureActiveContext)
#' @param Verbose - Print Tracing information (Default False)
#' @rdname AzurePutBlob
#' @export
AzurePutBlob <- function(AzureActiveContext,StorageAccount,StorageKey,Container,Blob,ResourceGroup, Contents="",File="",SubscriptionID,AzToken,Verbose=TRUE) {

  if(missing(SubscriptionID)) {SUBIDI <- AzureActiveContext$SubscriptionID} else (SUBIDI = SubscriptionID)
  if(missing(AzToken)) {ATI <- AzureActiveContext$Token} else (ATI = AzToken)
  if(missing(ResourceGroup)) {RGI <- AzureActiveContext$ResourceGroup} else (RGI = ResourceGroup)
  if(missing(StorageAccount)) {SAI <- AzureActiveContext$StorageAccount} else (SAI = StorageAccount)
  if(missing(StorageKey)) {STK <- AzureActiveContext$StorageKey} else (STK = StorageKey)
  if(missing(Container)) {CNTR <- AzureActiveContext$Container} else (CNTR = Container)
  if(missing(Blob)) {BLOBI <- AzureActiveContext$Blob} else (BLOBI = Blob)
  verbosity <- if(verbose) httr::verbose(TRUE) else NULL

  if (length(RGI)<1) {stop("Error: No ResourceGroup provided: Use ResourceGroup argument or set in AzureContext")}
  if (length(SAI)<1) {stop("Error: No StorageAccount provided: Use StorageAccount argument or set in AzureContext")}
  if (length(STK)<1) {stop("Error: No StorageKey provided: Use StorageKey argument or set in AzureContext")}
  if (length(CNTR)<1) {stop("Error: No Container provided: Use Container argument or set in AzureContext")}
  if (length(BLOBI)<1) {stop("Error: No Blob provided: Use Blob argument or set in AzureContext")}

  if(missing(Contents) && missing(File)) Stop("Content or File needs to be supplied")

  URL <- paste("http://",SAI,".blob.core.windows.net/",CNTR,"/",BLOBI,sep="")

  D1 <- Sys.getlocale("LC_TIME"); Sys.setlocale("LC_TIME", "us")
#  `x-ms-date` <- format(Sys.time(),"%a, %d %b %Y %H:%M:%S %Z", tz="GMT")
  Sys.setlocale("LC_TIME", D1)
  D1 <- format(Sys.time(),"%a, %d %b %Y %H:%M:%S %Z", tz="GMT")

  SIG <- GetSig(AzureActiveContext,URL,"PUT",STK,StorageAccount,ContentType = "text/plain; charset=UTF-8" , Size=nchar(Contents),Headers="x-ms-blob-type:BlockBlob",Container=CNTR,CMD=paste0("/",BLOBI),DateS=D1)

  AT <- paste0("SharedKey ", StorageAccount, ":", SIG)

  r <- PUT(URL,add_headers(.headers = c("Authorization" = AT, "Content-Length" = nchar(Contents) ,"x-ms-version"="2015-04-05","x-ms-date"=D1,"x-ms-blob-type"="BlockBlob","Content-Type" = "text/plain; charset=UTF-8")),body = Contents,verbosity)

  AzureActiveContext$Blob <- BLOBI
  return(paste("Blob:", BLOBI," Saved:",nchar(Contents),"bytes written"))
}
