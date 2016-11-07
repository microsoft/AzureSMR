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
#' @param verbose Print Tracing information (Default False)
#' @rdname AzureSAGetKey
#' @export
AzureSAGetKey <- function(AzureActiveContext,StorageAccount,AzToken,ResourceGroup,SubscriptionID,verbose = FALSE) {
  AzureCheckToken(AzureActiveContext)

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
  AzureActiveContext$StorageAccountK <- StorageAccount
  AzureActiveContext$ResourceGroup <- ResourceGroup
  AzureActiveContext$StorageKey <- df$keys$value[1]
  return(AzureActiveContext$StorageKey)
}

#' @name AzureSM: AzureCreateStorageAccount
#' @title Create an Azure Storage Account
#' @param AzureActiveContext Azure Context Object
#' @param StorageAccount StorageAccount
#' @param Token Token Object (or use AzureActiveContext)
#' @param ResourceGroup ResourceGroup Object (or use AzureActiveContext)
#' @param verbose Print Tracing information (Default False)
#' @rdname AzureSAGetKey
#' @export
AzureCreateStorageAccount <- function(AzureActiveContext,StorageAccount,AzToken,ResourceGroup,SubscriptionID,verbose = FALSE) {
  AzureCheckToken(AzureActiveContext)

  if(missing(ResourceGroup)) {RGI <- AzureActiveContext$ResourceGroup} else (RGI = ResourceGroup)
  if(missing(AzToken)) {ATI <- AzureActiveContext$Token} else (ATI = AzToken)

  if(missing(SubscriptionID)) {SUBIDI <- AzureActiveContext$SubscriptionID} else (SUBIDI = SubscriptionID)
  if (!length(StorageAccount)) {stop("Error: No Valid StorageAccount provided")}
  if (length(RGI)<1) {stop("Error: No ResourceGroup provided: Use ResourceGroup argument or set in AzureContext")}
  if (!length(ATI)) {stop("Error: No Token / Not currently Authenticated.")}
  if (!length(SUBIDI)) {stop("Error: No SubscriptionID provided: Use SUBID argument or set in AzureContext")}
  verbosity <- if(verbose) httr::verbose(TRUE) else NULL

  #https://management.azure.com/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.Storage/storageAccounts/{accountName}?api-version={api-version}

  bodyI = '
      {
   	"location": "northeurope",
  "tags": {
  "key1": "value1"
  },
  "properties": {
  "accessTier": "Hot"
  },
  "sku": {
  "name": "Standard_LRS"
  },
  "kind": "Storage"
}'

  bodyI <- '{
   	"location": "northeurope",
  "sku": {
  "name": "Standard_LRS"
  }}'

  URL <- paste("https://management.azure.com/subscriptions/",SUBIDI,"/resourceGroups/",RGI,"/providers/Microsoft.Storage/storageAccounts/",StorageAccount,"?api-version=2016-01-01",sep="")

  r <- PUT(URL,add_headers(.headers = c("Host" = "management.azure.com" ,"Authorization" = ATI, "Content-Type" = "application/json")),body = bodyI, encode = "json",verbosity)

  if (status_code(r) == 409)
  {
    warning("409: Conflict : Account already exists with the same name")
  return(NULL)
  }


  if (status_code(r) == 200) {print("Account already exists with the same properties")}
  else
    if (status_code(r) != 202) {stop(paste0("Error: Return code(",status_code(r),")" ))}

  rl <- content(r,"text",encoding="UTF-8")
  AzureActiveContext$StorageAccount <- StorageAccount
  AzureActiveContext$ResourceGroup <- ResourceGroup
  return("Create request Accepted. It can take a few moments to provision the storage account")
}

#' @name AzureSM: AzureDeleteStorageAccount
#' @title Delete an Azure Storage Account
#' @param AzureActiveContext Azure Context Object
#' @param StorageAccount StorageAccount
#' @param Token Token Object (or use AzureActiveContext)
#' @param ResourceGroup ResourceGroup Object (or use AzureActiveContext)
#' @param verbose Print Tracing information (Default False)
#' @rdname AzureSAGetKey
#' @export
AzureDeleteStorageAccount <- function(AzureActiveContext,StorageAccount,AzToken,ResourceGroup,SubscriptionID,verbose = FALSE) {
  AzureCheckToken(AzureActiveContext)

  if(missing(ResourceGroup)) {RGI <- AzureActiveContext$ResourceGroup} else (RGI = ResourceGroup)
  if(missing(AzToken)) {ATI <- AzureActiveContext$Token} else (ATI = AzToken)

  if(missing(SubscriptionID)) {SUBIDI <- AzureActiveContext$SubscriptionID} else (SUBIDI = SubscriptionID)
  if (!length(StorageAccount)) {stop("Error: No Valid StorageAccount provided")}
  if (length(RGI)<1) {stop("Error: No ResourceGroup provided: Use ResourceGroup argument or set in AzureContext")}
  if (!length(ATI)) {stop("Error: No Token / Not currently Authenticated.")}
  if (!length(SUBIDI)) {stop("Error: No SubscriptionID provided: Use SUBID argument or set in AzureContext")}
  verbosity <- if(verbose) httr::verbose(TRUE) else NULL


  URL <- paste("https://management.azure.com/subscriptions/",SUBIDI,"/resourceGroups/",RGI,"/providers/Microsoft.Storage/storageAccounts/",StorageAccount,"?api-version=2016-01-01",sep="")

  r <- DELETE(URL,add_headers(.headers = c("Host" = "management.azure.com" ,"Authorization" = ATI, "Content-Type" = "application/json")),verbosity)

  if (status_code(r) == 204) {stop(paste0("Error: Storage Account not found" ))}
  if (status_code(r) == 409) {stop(paste0("Error: An operation for the storage account is in progress." ))}
  if (status_code(r) != 200) {stop(paste0("Error: Return code(",status_code(r),")" ))}

  rl <- content(r,"text",encoding="UTF-8")
  AzureActiveContext$StorageAccount <- StorageAccount
  AzureActiveContext$ResourceGroup <- ResourceGroup
  return("Done")
  }

#' @name AzureSM: AzureListSAContainers
#' @title List Storage Containers for Specified Storage Account
#' @param AzureActiveContext Azure Context Object
#' @param StorageAccount StorageAccount
#' @param StorageKey StorageKey
#' @param ResourceGroup ResourceGroup Object (or use AzureActiveContext)
#' @param Token Token Object (or use AzureActiveContext)
#' @param SubscriptionID SubscriptionID Object (or use AzureActiveContext)
#' @param verbose Print Tracing information (Default False)
#' @rdname AzureListSAContainers
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
    STK <- AzureSAGetKey(sc,ResourceGroup = RGI,StorageAccount = SAI)
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
#' @name AzureSM: AzureListSABlobs
#' @title List Storage Containers for Specified Storage Account
#' @param AzureActiveContext Azure Context Object
#' @param StorageAccount StorageAccount
#' @param StorageKey StorageKey
#' @param Container Storage Container
#' @param ResourceGroup ResourceGroup Object (or use AzureActiveContext)
#' @param SubscriptionID SubscriptionID Object (or use AzureActiveContext)
#' @param AzToken Token Object (or use AzureActiveContext)
#' @param verbose Print Tracing information (Default False)
#' @rdname AzureListSABlobs
#' @export
AzureListSABlobs <- function(AzureActiveContext,StorageAccount,StorageKey,Container,ResourceGroup,SubscriptionID,AzToken,verbose = FALSE) {
  AzureCheckToken(AzureActiveContext)

  if(missing(SubscriptionID)) {SUBIDI <- AzureActiveContext$SubscriptionID} else (SUBIDI = SubscriptionID)
  if(missing(AzToken)) {ATI <- AzureActiveContext$Token} else (ATI = AzToken)
  if(missing(ResourceGroup)) {RGI <- AzureActiveContext$ResourceGroup} else (RGI = ResourceGroup)
  if(missing(StorageAccount)) {SAI <- AzureActiveContext$StorageAccount} else (SAI = StorageAccount)
  if(missing(StorageKey)) {STK <- AzureActiveContext$StorageKey} else (STK = StorageKey)
  if(missing(Container)) {CNTR <- AzureActiveContext$Container} else (CNTR = Container)
  verbosity <- if(verbose) httr::verbose(TRUE) else NULL

  if (length(RGI)<1) {stop("Error: No ResourceGroup provided: Use ResourceGroup argument or set in AzureContext")}
  if (length(SAI)<1) {stop("Error: No StorageAccount provided: Use StorageAccount argument or set in AzureContext")}
  if (length(CNTR)<1) {stop("Error: No Container provided: Use Container argument or set in AzureContext")}

  if (length(AzureActiveContext$StorageAccountK) <1 || SAI != AzureActiveContext$StorageAccountK|| length(AzureActiveContext$StorageKey) <1)
  {
    print("Fetching Storage Key..")
    STK <- AzureSAGetKey(sc,ResourceGroup = RGI,StorageAccount = SAI)
  }
  else
    STK <- AzureActiveContext$StorageKey

  if (length(STK)<1) {stop("Error: No StorageKey provided: Use StorageKey argument or set in AzureContext")}

  URL <- paste("http://",SAI,".blob.core.windows.net/",CNTR,"?restype=container&comp=list",sep="")
  D1 <- Sys.getlocale("LC_TIME"); Sys.setlocale("LC_TIME", "us")
#    `x-ms-date` <- format(Sys.time(),"%a, %d %b %Y %H:%M:%S %Z", tz="GMT")
  Sys.setlocale("LC_TIME", D1)
  D1 <- format(Sys.time(),"%a, %d %b %Y %H:%M:%S %Z", tz="GMT")

  SIG <- GetSig(AzureActiveContext,URL,"GET",STK,SAI,Container=CNTR,CMD="\ncomp:list\nrestype:container",DateS=D1)

  AT <- paste0("SharedKey ", SAI, ":", SIG)
    #  GetSig <- function(AzureActiveContext,url, verb, key, StorageAccount,Headers=NULL,Container=NULL,CMD=NULL,Size=NULL) {
  r <- GET(URL,add_headers(.headers = c("Authorization" = AT, "Content-Length" = "0","x-ms-version"="2015-04-05","x-ms-date"=D1)),verbosity)


  if (status_code(r) == 404) {
    warning("Container not found");
    return(NULL)
  }
  else
    if (status_code(r) != 200) {stop(paste0("Error: Return code(",status_code(r),")" ))}

  r <- content(r,"text",encoding="UTF-8")

  y<-htmlParse(r,encoding="UTF-8")

  namesx <- xpathApply(y, "//blobs//blob//name",xmlValue)

  if (length(namesx) == 0) {
    warning("Container is empty");
    return(NULL)
  }

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
#' @param Blob Blob Directory (leave for current directory)
#' @param Blob Blob Name
#' @param Blob Type ("Text" or "Raw")
#' @param StorageAccount StorageAccount
#' @param StorageKey StorageKey
#' @param Container Storage Container
#' @param ResourceGroup ResourceGroup Object (or use AzureActiveContext)
#' @param Token Token Object (or use AzureActiveContext)
#' @param SubscriptionID SubscriptionID Object (or use AzureActiveContext)
#' @param verbose Print Tracing information (Default False)
#' @rdname AzureGetBlob
#' @export
#' @importFrom digest hmac
#' @importFrom base64enc base64encode base64decode

AzureGetBlob <- function(AzureActiveContext,Blob, Directory,Type="text",StorageAccount,StorageKey,Container,ResourceGroup,SubscriptionID,AzToken,verbose=FALSE) {
  AzureCheckToken(AzureActiveContext)
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
  if (length(CNTR)<1) {stop("Error: No Container provided: Use Container argument or set in AzureContext")}
  if (length(BLOBI)<1) {stop("Error: No Blob provided: Use Blob argument or set in AzureContext")}

  if (length(AzureActiveContext$StorageAccountK) <1 || SAI != AzureActiveContext$StorageAccountK|| length(AzureActiveContext$StorageKey) <1)
  {
    cat("Fetching Storage Key..")
    STK <- AzureSAGetKey(sc,ResourceGroup = RGI,StorageAccount = SAI)
  }
  else
    STK <- AzureActiveContext$StorageKey

  if (length(STK)<1) {stop("Error: No StorageKey provided: Use StorageKey argument or set in AzureContext")}

  DIR <- AzureActiveContext$Directory
  DC <- AzureActiveContext$DContainer

  if(missing(Directory))
  {
    if (length(DIR)<1) DIR = "" # No previous Dir value
    if (length(DC)<1)
    {
      DIR <- ""  # No previous Dir value
      DC <- ""
    }
    else
      if (CNTR != DC) DIR = "" # Change of Container
  }
  else
    DIR <- Directory

  if (nchar(DIR) > 0) DIR <- paste0(DIR,"/")

  BLOBI <- paste0(DIR,BLOBI)
  BLOBI <- gsub("^/","",BLOBI)
  BLOBI <- gsub("^\\./","",BLOBI)
  cat(BLOBI)

  URL <- paste("http://",SAI,".blob.core.windows.net/",CNTR,"/",BLOBI,sep="")


  D1 <- Sys.getlocale("LC_TIME"); Sys.setlocale("LC_TIME", "C")
  #`x-ms-date` <- format(Sys.time(),"%a, %d %b %Y %H:%M:%S %Z", tz="GMT")
  Sys.setlocale("LC_TIME", D1)
  D1 <- format(Sys.time(),"%a, %d %b %Y %H:%M:%S %Z", tz="GMT")

  SIG <- GetSig(AzureActiveContext,URL,"GET",STK,SAI,Container=CNTR,CMD=paste0("/",BLOBI),DateS=D1)

  AT <- paste0("SharedKey ", SAI, ":", SIG)

  r <- GET(URL,add_headers(.headers = c("Authorization" = AT, "Content-Length" = "0","x-ms-version"="2015-04-05","x-ms-date"=D1)),verbosity)

  if (status_code(r) == 404) {
    cat(BLOBI)
    warning("File not found");
    return(NULL)
  }
  else
    if (status_code(r) != 200) {stop(paste0("Error: Return code(",status_code(r),")" ))}

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
#' @param Blob - Blob Name
#' @param Contents - Object to Store or Value
#' @param File - Local FileName to Store in Azure Blob
#' @param Directory - Directory location (leave for current directory)
#' @param StorageAccount - StorageAccount
#' @param StorageKey - StorageKey
#' @param Container - Storage Container
#' @param ResourceGroup ResourceGroup Object (or use AzureActiveContext)
#' @param Token - Token Object (or use AzureActiveContext)
#' @param SubscriptionID - SubscriptionID Object (or use AzureActiveContext)
#' @param verbose - Print Tracing information (Default False)
#' @rdname AzurePutBlob
#' @export
AzurePutBlob <- function(AzureActiveContext,Blob, Contents="",File="",Directory,StorageAccount,StorageKey,Container,ResourceGroup,SubscriptionID,AzToken,verbose=FALSE) {

  AzureCheckToken(AzureActiveContext)

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
  if (length(CNTR)<1) {stop("Error: No Container provided: Use Container argument or set in AzureContext")}
  if (length(BLOBI)<1) {stop("Error: No Blob provided: Use Blob argument or set in AzureContext")}

  DIR <- AzureActiveContext$Directory
  DC <- AzureActiveContext$DContainer

  if(missing(Directory))
  {

    if (length(DIR)<1) DIR = "" # No previous Dir value
    if (length(DC)<1)
    {
      DIR <- ""  # No previous Dir value
      DC <- ""
    }
    else
      if (CNTR != DC) DIR = "" # Change of Container
  }
  else
    DIR <- Directory
  if (nchar(DIR) > 0) DIR <- paste0(DIR,"/")

    BLOBI <- paste0(DIR,BLOBI)
  BLOBI <- gsub("^/","",BLOBI)
  BLOBI <- gsub("//","/",BLOBI)
  BLOBI <- gsub("//","/",BLOBI)

  if(missing(Contents) && missing(File)) stop("Content or File needs to be supplied")

  if(!missing(Contents) && !missing(File)) stop("Provided either Content OR File Argument")

  print(typeof(Contents))

  if (length(AzureActiveContext$StorageAccountK) <1 || SAI != AzureActiveContext$StorageAccountK|| length(AzureActiveContext$StorageKey) <1)  {
    cat("Fetching Storage Key..")
    STK <- AzureSAGetKey(sc,ResourceGroup = RGI,StorageAccount = SAI)
  }
  else
    STK <- AzureActiveContext$StorageKey

  if (length(STK)<1) {stop("Error: No StorageKey provided: Use StorageKey argument or set in AzureContext")}

  URL <- paste("http://",SAI,".blob.core.windows.net/",CNTR,"/",BLOBI,sep="")

  D1 <- Sys.getlocale("LC_TIME"); Sys.setlocale("LC_TIME", "C")
#  `x-ms-date` <- format(Sys.time(),"%a, %d %b %Y %H:%M:%S %Z", tz="GMT")
  Sys.setlocale("LC_TIME", D1)
  D1 <- format(Sys.time(),"%a, %d %b %Y %H:%M:%S %Z", tz="GMT")
  if (nchar(Contents) == 0) Contents <- "-"

  SIG <- GetSig(AzureActiveContext,URL,"PUT",STK,SAI,ContentType = "text/plain; charset=UTF-8" , Size=nchar(Contents),Headers="x-ms-blob-type:BlockBlob",Container=CNTR,CMD=paste0("/",BLOBI),DateS=D1)

    AT <- paste0("SharedKey ", SAI, ":", SIG)

  r <- PUT(URL,add_headers(.headers = c("Authorization" = AT, "Content-Length" = nchar(Contents) ,"x-ms-version"="2015-04-05","x-ms-date"=D1,"x-ms-blob-type"="BlockBlob","Content-Type" = "text/plain; charset=UTF-8")),body = Contents,verbosity)

#    r <- PUT(URL,add_headers(.headers = c("Authorization" = AT, "Content-Length" = nchar(Contents) ,"x-ms-version"="2015-04-05","x-ms-date"=D1,"x-ms-blob-type"="BlockBlob","Content-Type" = "text/plain; charset=UTF-8")),body = upload_file(path=  path.expand("c:\\temp\\fred"),type = 'text/csv'),verbosity)


  AzureActiveContext$Blob <- BLOBI
  return(paste("Blob:", BLOBI," Saved:",nchar(Contents),"bytes written"))
}


#' @name AzureSM: AzureCreateSAContainer
#' @title Create Storage Containers in a specified Storage Account
#' @param AzureActiveContext Azure Context Object
#' @param Container - Storage Container
#' @param StorageAccount StorageAccount
#' @param StorageKey StorageKey
#' @param ResourceGroup ResourceGroup Object (or use AzureActiveContext)
#' @param Token Token Object (or use AzureActiveContext)
#' @param SubscriptionID SubscriptionID Object (or use AzureActiveContext)
#' @param verbose Print Tracing information (Default False)
#' @rdname AzureCreateSAContainer
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
    STK <- AzureSAGetKey(sc,ResourceGroup = RGI,StorageAccount = SAI)
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

#' @name AzureSM: AzureDeleteSAContainer
#' @title Delete Storage Container in a specified Storage Account
#' @param AzureActiveContext Azure Context Object
#' @param Container - Storage Container
#' @param StorageAccount StorageAccount
#' @param StorageKey StorageKey
#' @param ResourceGroup ResourceGroup Object (or use AzureActiveContext)
#' @param Token Token Object (or use AzureActiveContext)
#' @param SubscriptionID SubscriptionID Object (or use AzureActiveContext)
#' @param verbose Print Tracing information (Default False)
#' @rdname AzureDeleteSAContainer
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
    STK <- AzureSAGetKey(sc,ResourceGroup = RGI,StorageAccount = SAI)
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

#' @name AzureSM: AzureBlobLS
#' @title List Blob files in a Storage account directory
#' @param AzureActiveContext Azure Context Object
#' @param Directory - Set Directory to list
#' @param Recursive - List directories recursively (Default FALSE)
#' @param StorageAccount StorageAccount
#' @param StorageKey StorageKey
#' @param Container Storage Container
#' @param ResourceGroup ResourceGroup Object (or use AzureActiveContext)
#' @param Token Token Object (or use AzureActiveContext)
#' @param SubscriptionID SubscriptionID Object (or use AzureActiveContext)
#' @param verbose Print Tracing information (Default False)
#' @rdname AzureBlobLS
#' @export
#' @importFrom digest hmac
#' @importFrom base64enc base64encode base64decode
AzureBlobLS <- function(AzureActiveContext,Directory,Recursive=FALSE,StorageAccount,StorageKey,Container,ResourceGroup,SubscriptionID,AzToken,verbose=FALSE) {
  AzureCheckToken(AzureActiveContext)
  if(missing(SubscriptionID)) {SUBIDI <- AzureActiveContext$SubscriptionID} else (SUBIDI = SubscriptionID)
  if(missing(AzToken)) {ATI <- AzureActiveContext$Token} else (ATI = AzToken)
  if(missing(ResourceGroup)) {RGI <- AzureActiveContext$ResourceGroup} else (RGI = ResourceGroup)
  if(missing(StorageKey)) {STK <- AzureActiveContext$StorageKey} else (STK = StorageKey)
  if(missing(StorageAccount)) {SAI <- AzureActiveContext$StorageAccount} else (SAI = StorageAccount)
  if(missing(Container)) {CNTR <- AzureActiveContext$Container} else (CNTR = Container)
  if(missing(Directory)) {DIR <- AzureActiveContext$Directory} else (DIR = Directory)
  verbosity <- if(verbose) httr::verbose(TRUE) else NULL

  if (length(RGI)<1) {stop("Error: No ResourceGroup provided: Use ResourceGroup argument or set in AzureContext")}
  if (length(SAI)<1) {stop("Error: No StorageAccount provided: Use StorageAccount argument or set in AzureContext")}
  if (length(CNTR)<1) {stop("Error: No Container provided: Use Container argument or set in AzureContext")}
  SD <- 0

  if(missing(Directory))
  {
    DIR <- AzureActiveContext$Directory
    DC <- AzureActiveContext$DContainer
    if (length(DC) <1) DC <- ""
        if (length(DIR) < 1 ) DIR = "/"
    if (DC != CNTR) DIR = "/" # Change of Container
  }
  else
  {
    if (substr(Directory,1,1) != "/")
    {
      DIR2 <- AzureActiveContext$Directory
      if (length(DIR2)>0)
      {
        DIR <- paste0(DIR2,"/",Directory)
        SD <- 1
        DIR <- gsub("\\./","",DIR)
      }
    }
  }

  if (length(DIR)<1) {DIR="/"}
  if (substr(DIR,1,1) != "/") DIR <- paste0("/",DIR)

  DIR <- gsub("//","/",DIR)

  if (length(AzureActiveContext$StorageAccountK) <1 || SAI != AzureActiveContext$StorageAccountK|| length(AzureActiveContext$StorageKey) <1)
  {
    cat("Fetching Storage Key..")
    STK <- AzureSAGetKey(sc,ResourceGroup = RGI,StorageAccount = SAI)
  }
  else
    STK <- AzureActiveContext$StorageKey


  if (length(STK)<1) {stop("Error: No StorageKey provided: Use StorageKey argument or set in AzureContext")}

  AzureActiveContext$DirContainer <- CNTR

  Files <- AzureListSABlobs(AzureActiveContext,Container = CNTR)

  Files$Name <- paste0("/",Files$Name)
  Files$Name <- gsub("//","/",Files$Name)
  Files$Name <- gsub("//","/",Files$Name)

  F1 <- grep(paste0("^",DIR),Files$Name)

  if (SD == 0 ) AzureActiveContext$Directory = DIR
  AzureActiveContext$Container = CNTR
  AzureActiveContext$DContainer = CNTR
  cat(paste0("Current Directory - ",SAI," >  ",CNTR, " : " ,DIR,"\n\n"))

  DIR <- gsub("//","/",DIR)
  Depth <- length(strsplit(DIR,"/")[[1]])
  FO <- data.frame()
  Prev <- ""
  if (Recursive == TRUE)
  {
    Files <- Files[F1,]
    return(Files)
  }
  else
  {
    Files <- Files[F1,]
    rownames(Files) <- NULL
    F2 <- strsplit(Files$Name, "/")
    f1 <- 1
    f2 <- 1
    for (RO in F2){
      if (length(RO) > Depth)
      {
        if (length(RO) >= Depth +1)   # Check Depth Level
        {
          if (Prev != RO[Depth+1])
          {
#            print(paste0("./",RO[Depth+1],"/"))
            if(length(RO) > Depth +1)
            {
              FR <- data.frame(paste0("./",RO[Depth+1]),"Directory",Files[f1,2:4],stringsAsFactors=FALSE)
              FR[,4] <- "-"  # Second file name found so assumed blob was a directory

            }
            else
              FR <- data.frame(paste0("./",RO[Depth+1]),"File",Files[f1,2:4],stringsAsFactors=FALSE)

            colnames(FR)[2] <- "Type"

            FO <- rbind(FO,FR)

            f2 = f2 + 1
          }
          else
          {
            FO[f2-1,2] <- "Directory"  # Second file name found so assumed blob was a directory
            FO[f2-1,4] <- "-"  # Second file name found so assumed blob was a directory

          }
        }
      }
      Prev <- RO[Depth+1]
      if (is.na(Prev)) Prev <- ""
      f1 = f1 + 1
    }
    if (f2 == 1)
    {
      if (SD==0)
        warning("Directory not found")
      else
        warning("No Files found")
      return(NULL)
    }
    AzureActiveContext$Directory = DIR
    AzureActiveContext$Container = CNTR
    AzureActiveContext$StorageAccount = SAI
    AzureActiveContext$ResourceGroup = RGI
    AzureActiveContext$DContainer = CNTR

    rownames(FO) <- NULL
    colnames(FO)[1] <- "FileName"
    colnames(FO)[2] <- "Type"
    FN <- grep("Directory",FO$Type)   # Suffix / to Directory names
    FO$FileName[FN] <- paste0(FO$FileName[FN],"/")
        return(FO)
  }
}

#' @name AzureSM: AzureBlobFind
#' @title Find File in a Storage account directory
#' @param AzureActiveContext Azure Context Object
#' @param StorageAccount StorageAccount
#' @param StorageKey StorageKey
#' @param Container Storage Container
#' @param ResourceGroup ResourceGroup Object (or use AzureActiveContext)
#' @param Token Token Object (or use AzureActiveContext)
#' @param SubscriptionID SubscriptionID Object (or use AzureActiveContext)
#' @param verbose Print Tracing information (Default False)
#' @rdname AzureBlobFind
#' @export
#' @importFrom digest hmac
#' @importFrom base64enc base64encode base64decode
AzureBlobFind <- function(AzureActiveContext,File,StorageAccount,StorageKey,Container,ResourceGroup,SubscriptionID,AzToken,verbose=FALSE) {
  AzureCheckToken(AzureActiveContext)
  if(missing(SubscriptionID)) {SUBIDI <- AzureActiveContext$SubscriptionID} else (SUBIDI = SubscriptionID)
  if(missing(AzToken)) {ATI <- AzureActiveContext$Token} else (ATI = AzToken)
  if(missing(ResourceGroup)) {RGI <- AzureActiveContext$ResourceGroup} else (RGI = ResourceGroup)
  if(missing(StorageAccount)) {SAI <- AzureActiveContext$StorageAccount} else (SAI = StorageAccount)
  if(missing(StorageKey)) {STK <- AzureActiveContext$StorageKey} else (STK = StorageKey)
#  if(missing(Container)) {CNTR <- AzureActiveContext$Container} else (CNTR = Container)
  if(missing(File)) {stop("Error: No Filename{pattern} provided")}
  verbosity <- if(verbose) httr::verbose(TRUE) else NULL

  if (length(RGI)<1) {stop("Error: No ResourceGroup provided: Use ResourceGroup argument or set in AzureContext")}
  if (length(SAI)<1) {stop("Error: No StorageAccount provided: Use StorageAccount argument or set in AzureContext")}


  if (length(AzureActiveContext$StorageAccountK) <1 || SAI != AzureActiveContext$StorageAccountK|| length(AzureActiveContext$StorageKey) <1)
  {
    print("Fetching Storage Key..")
    STK <- AzureSAGetKey(sc,ResourceGroup = RGI,StorageAccount = SAI)
  }
  else
    STK <- AzureActiveContext$StorageKey

  if (length(STK)<1) {stop("Error: No StorageKey provided: Use StorageKey argument or set in AzureContext")}

  if (!missing(Container)) {
    CL <- Container
  }
  else
  {
    CL <- AzureListSAContainers(AzureActiveContext)$Name
  }

  F2 <- data.frame()
  for (CI in CL){
    Files <- AzureListSABlobs(sc,Container = CI)
    Files$Name <- paste0("/",Files$Name)
    #    print(Files$Name)
    F1 <- grep(File,Files$Name)
    Files <- Files[F1,1:4]
#    Files$Container <- CI
    Files <- cbind(Container = CI, Files)
    F2 <- rbind(F2, Files)
  }
  rownames(F2) <- NULL
  return(F2)
}

#' @name AzureSM: AzureBlobCD
#' @title Azure Blob change current Directory
#' @param AzureActiveContext Azure Context Object
#' @param Directory - Set Directory to Path (Supports (../ and ../..))
#' @param Container Storage Container
#' @param StorageAccount StorageAccount
#' @param StorageKey StorageKey
#' @param ResourceGroup ResourceGroup Object (or use AzureActiveContext)
#' @param Token Token Object (or use AzureActiveContext)
#' @param SubscriptionID SubscriptionID Object (or use AzureActiveContext)
#' @param verbose Print Tracing information (Default False)
#' @rdname AzureBlobCD
#' @export
#' @importFrom digest hmac
#' @importFrom base64enc base64encode base64decode
AzureBlobCD <- function(AzureActiveContext,Directory,Container,File,StorageAccount,StorageKey,ResourceGroup,SubscriptionID,AzToken,verbose=FALSE) {
  AzureCheckToken(AzureActiveContext)
  if(missing(SubscriptionID)) {SUBIDI <- AzureActiveContext$SubscriptionID} else (SUBIDI = SubscriptionID)
  if(missing(AzToken)) {ATI <- AzureActiveContext$Token} else (ATI = AzToken)
  if(missing(ResourceGroup)) {RGI <- AzureActiveContext$ResourceGroup} else (RGI = ResourceGroup)
  if(missing(StorageAccount)) {SAI <- AzureActiveContext$StorageAccount} else (SAI = StorageAccount)
  if(missing(StorageKey)) {STK <- AzureActiveContext$StorageKey} else (STK = StorageKey)
  if(missing(Container)) {CNTR <- AzureActiveContext$Container} else (CNTR = Container)
  verbosity <- if(verbose) httr::verbose(TRUE) else NULL

  if (length(RGI)<1) {stop("Error: No ResourceGroup provided: Use ResourceGroup argument or set in AzureContext")}
  if (length(SAI)<1) {stop("Error: No StorageAccount provided: Use StorageAccount argument or set in AzureContext")}

  if(missing(Directory))
  {
     DIR <- AzureActiveContext$Directory
     DC <- AzureActiveContext$DContainer
     if (length(DIR)<1) DIR = "/" # No previous Dir value
     if (length(DC)<1)
     {
       DIR <- "/"  # No previous Dir value
       DC <- ""
     }
     else
     if (CNTR != DC) DIR = "/" # Change of Container

     AzureActiveContext$Directory = DIR
     AzureActiveContext$Container = CNTR
     AzureActiveContext$StorageAccount = SAI
     AzureActiveContext$ResourceGroup = RGI
     AzureActiveContext$DContainer = CNTR
     return(paste0("Current Directory - ",SAI," >  ",CNTR, " : " ,DIR))

  }
  if (length(AzureActiveContext$StorageAccountK) <1 || SAI != AzureActiveContext$StorageAccountK|| length(AzureActiveContext$StorageKey) <1)
  {
    print("Fetching Storage Key..")
    STK <- AzureSAGetKey(sc,ResourceGroup = RGI,StorageAccount = SAI)
  }
  if (length(STK)<1) {stop("Error: No StorageKey provided: Use StorageKey argument or set in AzureContext")}

  if (Directory == "../" || Directory == "..")   # Basic attempt at relative paths
    Directory <- gsub("/[a-zA-Z0-9]*$","",AzureActiveContext$Directory)

  if (Directory == "../..")
  {
    Directory <- gsub("/[a-zA-Z0-9]*$","",AzureActiveContext$Directory)
    Directory <- gsub("/[a-zA-Z0-9]*$","",Directory)
  }

  if (Directory == "../../..")
  {
    Directory <- gsub("/[a-zA-Z0-9]*$","",AzureActiveContext$Directory)
    Directory <- gsub("/[a-zA-Z0-9]*$","",Directory)
    Directory <- gsub("/[a-zA-Z0-9]*$","",Directory)
  }

  AzureActiveContext$Directory = Directory
  AzureActiveContext$Container = CNTR
  AzureActiveContext$DContainer = CNTR
  AzureActiveContext$StorageAccount = SAI
  AzureActiveContext$ResourceGroup = RGI
  AzureActiveContext$DContainer = CNTR

  return(paste0("Current Directory - ",SAI," >  ",CNTR, " : " ,Directory))
}

#' @name AzureSM: AzureListSA
#' @title List Storage accounts
#' @param AzureActiveContext Azure Context Object
#' @param ResourceGroup ResourceGroup Object (or use AzureActiveContext)
#' @param SubscriptionID SubscriptionID Object (or use AzureActiveContext)
#' @param AzToken Token Object (or use AzureActiveContext)
#' @param verbose Print Tracing information (Default False)
#' @rdname AzureListSA
#' @export
AzureListSA <- function(AzureActiveContext,ResourceGroup,SubscriptionID,AzToken,verbose = FALSE)
{
  if(missing(ResourceGroup)) {RGI <- AzureActiveContext$ResourceGroup} else (RGI = ResourceGroup)
  if(missing(AzToken)) {ATI <- AzureActiveContext$Token} else (ATI = AzToken)

  if(missing(SubscriptionID)) {SUBIDI <- AzureActiveContext$SubscriptionID} else (SUBIDI = SubscriptionID)
  if (length(RGI)<1) {stop("Error: No ResourceGroup provided: Use ResourceGroup argument or set in AzureContext")}
  if (!length(ATI)) {stop("Error: No Token / Not currently Authenticated.")}
  if (!length(SUBIDI)) {stop("Error: No SubscriptionID provided: Use SUBID argument or set in AzureContext")}
  verbosity <- if(verbose) httr::verbose(TRUE) else NULL
  if(missing(ResourceGroup))
    SA <- AzureListAllRecources(AzureActiveContext, Type="Microsoft.Storage/storageAccounts")
  else
    SA <- AzureListAllRecources(AzureActiveContext, Type="Microsoft.Storage/storageAccounts",ResourceGroup ="myResourceGroup")
  rownames(SA) <- NULL
  return(SA)
}
