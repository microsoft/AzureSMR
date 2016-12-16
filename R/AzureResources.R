#' @name AzureSM: AzureAuthenticate
#' @title Authenticates against Azure Active Directory application
#' @param AzureActiveContext Azure Context Object
#' @param TID Tenant ID Object
#' @param CID Client ID Object
#' @param KEY Authentication KEY Object
#' @param verbose Print Tracing information (Default False)
#' @note See \url{https://azure.microsoft.com/en-us/documentation/articles/resource-group-create-service-principal-portal/} to learn how to set up an Active Directory application
#' @references \url{https://azure.microsoft.com/en-us/documentation/articles/resource-group-create-service-principal-portal/}
#' @rdname AzureAuthenticate
#' @name AzureAuthenticate
#' @return Retunrs Azure Tokem and sets AzureContext Token
#' @export
AzureAuthenticate <- function(AzureActiveContext,TID, CID, KEY,verbose = FALSE) {

  if(missing(TID)) {ATID <- AzureActiveContext$TID} else (ATID = TID)
  if(missing(CID)) {ACID <- AzureActiveContext$CID} else (ACID = CID)
  if(missing(KEY)) {AKEY <- AzureActiveContext$KEY} else (AKEY = KEY)

  if (!length(ATID)) {stop("Error: No TID provided: Use TID argument or set in AzureContext")}
  if (!length(ACID)) {stop("Error: No CID provided: Use CID argument or set in AzureContext")}
  if (!length(AKEY)) {stop("Error: No KEY provided: Use KEY argument or set in AzureContext")}
  verbosity <- if(verbose) httr::verbose(TRUE) else NULL

  URLGT <- paste0("https://login.microsoftonline.com/",ATID,"/oauth2/token?api-version=1.0")

  bodyGT <- paste0("grant_type=client_credentials&resource=https%3A%2F%2Fmanagement.azure.com%2F&client_id=",ACID,"&client_secret=",AKEY)

  r <- httr::POST(URLGT,
                  add_headers(
                    .headers = c("Cache-Control" = "no-cache",
                                 "Content-Type" = "application/x-www-form-urlencoded")),
                  body=bodyGT,
                  verbosity)
  if (status_code(r) != 200) {
    j1 <- content(r, "parsed",encoding="UTF-8")
    message(j1$error)
    message(j1$error_description)
    stop(paste("Error: Return code",status_code(r) ))
  }

  AT <- paste("Bearer",j1$access_token)

  AzureActiveContext$Token  <- AT
  AzureActiveContext$TID    <- ATID
  AzureActiveContext$CID    <- ACID
  AzureActiveContext$KEY    <- AKEY
  AzureActiveContext$EXPIRY <- Sys.time() + 3598
  SUBS <- AzureListSubscriptions(AzureActiveContext)
  return("Authentication Suceeded : Key Obtained")
}

#' @name AzureSM: AzureCheckToken
#' @title Check the timestamp of a Token and Renew if needed.
#' @param AzureActiveContext Azure Context Object
#' @export
AzureCheckToken <- function(AzureActiveContext) {
  if (is.null(AzureActiveContext$EXPIRY)) print (stop("Not Authenticated: Use AzureAuthenticate"))

  if (AzureActiveContext$EXPIRY < Sys.time())
  {
    print("Azure Token Expired: Attempting automatic renewal")
    AzureAuthenticate(AzureActiveContext)
  }
  return("OK")
}

#' @name AzureSM: AzureListSubscriptions
#' @title  Get available Subscriptions
#' @description  Get available Subscriptions
#' @param Azure Context Object
#' @param Token Token Object (or use AzureActiveContext)
#' @param Verbose Print Tracing information (Default False)
#' @rdname AzureListSubscriptions
#' @return Returns Dataframe of SubscriptionID sets AzureContext SubscriptionID
#' @export
AzureListSubscriptions <- function(AzureActiveContext,ATI,verbose = FALSE) {
  if(missing(ATI)) {AT <- AzureActiveContext$Token} else (AT = ATI)
  if (nchar(AT)<5) {stop("Error: No Token / Not currently Authenticated.")}
  verbosity <- if(verbose) httr::verbose(TRUE) else NULL

  URLGS <- "https://management.azure.com/subscriptions?api-version=2015-01-01"

  r <- GET(URLGS,add_headers(.headers = c("Host" = "management.azure.com" ,"Authorization" = AT, "Content-Type" = "application/json")),verbosity)
  dfs <- lapply(content(r), data.frame, stringsAsFactors = FALSE)
  #head(dfs)
  colnames(dfs)
  typeof(dfs)
  df1 <- rbind.fill(dfs)
  if (nrow(df1) == 1) AzureActiveContext$SubscriptionID <- df1[,2]
  return(df1)
}

#' @name AzureSM: AzureListRG
#' @title Get all Resource Groups in default Subscription
#' @param AzureActiveContext Azure Context Object
#' @param Token Token Object (or use AzureActiveContext)
#' @param SubscriptionID SubscriptionID Object (or use AzureActiveContext)
#' @param Verbose Print Tracing information (Default False)
#' @rdname AzureListRG
#' @return Returns Dataframe of ResourceGroups
#' @export
AzureListRG <- function(AzureActiveContext,SUBID,AT,verbose = FALSE) {
  AzureCheckToken(AzureActiveContext)
  if(missing(AT)) {ATI <- AzureActiveContext$Token} else (ATI = AT)
  if(missing(SUBID)) {SUBIDI <- AzureActiveContext$SubscriptionID} else (SUBIDI = SUBID)
  if (!length(ATI)) {stop("Error: No Token / Not currently Authenticated.")}
  if (!length(SUBIDI)) {stop("Error: No SubscriptionID provided: Use SUBID argument or set in AzureContext")}
  verbosity <- if(verbose) httr::verbose(TRUE) else NULL

  URLRG <- paste("https://management.azure.com/subscriptions/",SUBIDI,"/resourcegroups?api-version=2015-01-01",sep="")

  r <- GET(URLRG,add_headers(.headers = c("Host" = "management.azure.com" ,"Authorization" = ATI, "Content-Type" = "application/json")),verbosity)
  if (status_code(r) != 200) {stop(paste("Error: Return code",status_code(r) ))}

  rl <- content(r,"text",encoding="UTF-8")
  df <- fromJSON(rl)
  dfn <- as.data.frame(paste(df$value$name))

  clust <- nrow(dfn)
  dfn[1:clust,1] <- df$value$name
  dfn[1:clust,2] <- df$value$location
  dfn[1:clust,3] <- df$value$id
  colnames(dfn) <- c("Name", "Location","ID")
  return(dfn)
}

#' @name AzureSM: AzureListAllRecources
#' @title Get all Resource in default Subscription
#' @param AzureActiveContext Azure Context Object
#' @param ResourceGroup ResourceGroup Object (or use AzureActiveContext)
#' @param Token Token Object (or use AzureActiveContext)
#' @param SubscriptionID SubscriptionID Object (or use AzureActiveContext)
#' @param Verbose Print Tracing information (Default False)
#' @rdname AzureListAllRecources
#' @return Returns Dataframe of Resources
#' @export
AzureListAllRecources <- function(AzureActiveContext,ResourceGroup,SUBID,AT,Name, Type, Location,verbose = FALSE) {

  AzureCheckToken(AzureActiveContext)

  if(missing(AT)) {ATI <- AzureActiveContext$Token} else (ATI = AT)
  if(missing(SUBID)) {SUBIDI <- AzureActiveContext$SubscriptionID} else (SUBIDI = SUBID)
  #  if (ATI == "") stop("Token not provided")
  #  if (SUBIDI == "") stop("Subscription not provided")

  if (!length(ATI)) {stop("Error: No Token / Not currently Authenticated.")}
  if (!length(SUBIDI)) {stop("Error: No SubscriptionID provided: Use SUBID argument or set in AzureContext")}
  verbosity <- if(verbose) httr::verbose(TRUE) else NULL

  URLRS <- paste("https://management.azure.com/subscriptions/",SUBIDI,"/resources?api-version=2015-01-01",sep="")
  r <- GET(URLRS,add_headers(.headers = c("Host" = "management.azure.com" ,"Authorization" = ATI, "Content-Type" = "application/json")),verbosity)
  rl <- content(r,"text",encoding="UTF-8")
  #  print(rl)
  df <- fromJSON(rl)
  dfn <- as.data.frame(paste(df$value$name))
  dfn[2] <- df$value$type
  dfn[3] <- df$value$location
  dfn[4] <- df$value$id

  #print(df)

  colnames(dfn) <- c("Name", "Type", "Location", "ID")
  if (!missing(Name) > 0 )
    dfn <- dfn[grep(Name, dfn$Name), ]

  if (!missing(Type) > 0 )
  {
    dfn <- dfn[grep(Type, dfn$Type), ]
  }
  if (!missing(Location) > 0 )
  {
    dfn <- dfn[grep(Location, dfn$Location), ]
  }
  if (!missing(ResourceGroup) > 0 )
  {
    dfn <- dfn[grep(paste("/resourceGroups/",ResourceGroup,sep = ""), dfn$ID), ]
  }

  dfn$RG <- sapply(strsplit(as.character(dfn$ID), "/"), `[`, 5)

  dfn <- dfn[,c(1,2,5,3,4)]

  return(dfn)
}


#' @name AzureCreateResourceGroup: AzureListAllRecources
#' @title Create a ResourceGroup
#' @param AzureActiveContext - Azure Context Object
#' @param ResourceGroup - ResourceGroup Object (or use AzureActiveContext)
#' @param AT - Token Object (or use AzureActiveContext)
#' @param SUBID - SubscriptionID Object (or use AzureActiveContext)
#' @param Verbose - Print Tracing information (Default False)
#' @rdname AzureCreateResourceGroup
#' @return Returns Dataframe of Resources
#' @export
AzureCreateResourceGroup <- function(AzureActiveContext,ResourceGroup,Location,SUBID,AT,verbose=FALSE) {
  AzureCheckToken(AzureActiveContext)
  if(missing(AT)) {ATI <- AzureActiveContext$Token} else (ATI = AT)
  if(missing(SUBID)) {SUBIDI <- AzureActiveContext$SubscriptionID} else (SUBIDI = SUBID)
  #  if (ATI == "") stop("Token not provided")
  #  if (SUBIDI == "") stop("Subscription not provided")
  if(missing(ResourceGroup)) {RGI <- AzureActiveContext$ResourceGroup} else (RGI = ResourceGroup)
  if(missing(Location)) {stop("Error: No Location provided")}

  verbosity <- if(verbose) httr::verbose(TRUE) else NULL

  if (!length(RGI)) {stop("Error: No ResourceGroup provided: Use ResourceGroup argument or set in AzureContext")}
  if (!length(ATI)) {stop("Error: No Token / Not currently Authenticated.")}
  if (!length(SUBIDI)) {stop("Error: No SubscriptionID provided: Use SUBID argument or set in AzureContext")}

  bodyI <- list(location = Location)

  URLRS <- paste("https://management.azure.com/subscriptions/",SUBIDI,"/resourcegroups/",RGI,"?api-version=2015-01-01",sep="")
  r <- PUT(URLRS,add_headers(.headers = c("Host" = "management.azure.com" ,"Authorization" = ATI, "Content-Type" = "application/json")),body = bodyI, encode = "json",verbosity)
  if (status_code(r) != 201) {stop(paste("Error: Return code",status_code(r) ))}
  rl <- content(r,"text",encoding="UTF-8")
  df <- fromJSON(rl)
  if (length(df$error$code) && df$error$code == "LocationNotAvailableForResourceGroup")
    stop(df$message)

  return("Create Request Submitted")
}

#' @name AzureDeleteResourceGroup: AzureListAllRecources
#' @title Delete a ResourceGroup with all Resources
#' @param AzureActiveContext Azure Context Object
#' @param ResourceGroup ResourceGroup Object (or use AzureActiveContext)
#' @param SubscriptionID SubscriptionID Object (or use AzureActiveContext)
#' @param Token Token Object (or use AzureActiveContext)
#' @param Verbose Print Tracing information (Default False)
#' @rdname AzureDeleteResourceGroup
#' @return Returns Dataframe of Resources
#' @export
AzureDeleteResourceGroup <- function(AzureActiveContext,ResourceGroup,SUBID,AT, Type,verbose=FALSE) {
  AzureCheckToken(AzureActiveContext)

  if(missing(AT)) {ATI <- AzureActiveContext$Token} else (ATI = AT)
  if(missing(SUBID)) {SUBIDI <- AzureActiveContext$SubscriptionID} else (SUBIDI = SUBID)
  #  if (ATI == "") stop("Token not provided")
  #  if (SUBIDI == "") stop("Subscription not provided")
  if(missing(ResourceGroup)) stop("Please supply Resource Group to Confirm")
  if(missing(ResourceGroup)) {RGI <- AzureActiveContext$ResourceGroup} else (RGI = ResourceGroup)
  verbosity <- if(verbose) httr::verbose(TRUE) else NULL

  if (!length(RGI)) {stop("Error: No ResourceGroup provided: Use ResourceGroup argument or set in AzureContext")}
  if (!length(ATI)) {stop("Error: No Token / Not currently Authenticated.")}
  if (!length(SUBIDI)) {stop("Error: No SubscriptionID provided: Use SUBID argument or set in AzureContext")}

  URLRS <- paste("https://management.azure.com/subscriptions/",SUBIDI,"/resourcegroups/",RGI,"?api-version=2015-01-01",sep="")
  r <- DELETE(URLRS,add_headers(.headers = c("Host" = "management.azure.com" ,"Authorization" = ATI, "Content-Type" = "application/json")),verbosity)
  if (status_code(r) == 404) {stop(paste0("Error: Resource Group Not Found(",status_code(r),")" ))}
  if (status_code(r) != 202) {stop(paste0("Error: Return code(",status_code(r),")" ))}
  rl <- content(r,"text",encoding="UTF-8")
  if(nchar(rl)>1)
  {
    df <- fromJSON(rl)
    stop(df$error$message)
  }
  return("Delete Request Submitted")
}
