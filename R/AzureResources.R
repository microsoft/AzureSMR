
#' Get available Subscriptions.
#'
#' @inheritParams SetAzureContext
#' @inheritParams AzureAuthenticate
#'
#' @return Returns Dataframe of SubscriptionID sets AzureContext SubscriptionID
#' @family Resource group functions
#' @export
AzureListSubscriptions <- function(AzureActiveContext,AzToken,verbose = FALSE) {
  if(missing(AzToken)) {AT <- AzureActiveContext$Token} else (AT = AzToken)
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


#' Get all Resource Groups in default Subscription.
#'
#' @inheritParams SetAzureContext
#' @inheritParams AzureAuthenticate
#'
#' @return Returns Dataframe of ResourceGroups
#' @family Resource group functions
#' @export
AzureListRG <- function(AzureActiveContext,SubscriptionID,AzToken,verbose = FALSE) {
  AzureCheckToken(AzureActiveContext)
  if(missing(AzToken)) {ATI <- AzureActiveContext$Token} else (ATI = AzToken)
  if(missing(SubscriptionID)) {SUBIDI <- AzureActiveContext$SubscriptionID} else (SUBIDI = SubscriptionID)
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


#' Get all Resource in default Subscription.
#'
#' @inheritParams SetAzureContext
#' @inheritParams AzureAuthenticate
#'
#' @param AzureActiveContext Azure Context Object
#' @param ResourceGroup ResourceGroup Object (or use AzureActiveContext)
#' @param SubscriptionID SubscriptionID Object (or use AzureActiveContext)
#' @param Name Name
#' @param Type Type
#' @param Location Location string
#'
#' @return Returns Dataframe of Resources
#' @family Resource group functions
#' @export
AzureListAllResources <- function(AzureActiveContext,ResourceGroup,
                                  SubscriptionID,AzToken,Name, Type, Location,verbose = FALSE) {

  AzureCheckToken(AzureActiveContext)

  if(missing(AzToken)) {ATI <- AzureActiveContext$Token} else (ATI = AzToken)
  if(missing(SubscriptionID)) {SUBIDI <- AzureActiveContext$SubscriptionID} else (SUBIDI = SubscriptionID)
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


#' Create a ResourceGroup.
#'
#' @inheritParams SetAzureContext
#' @inheritParams AzureAuthenticate
#' @inheritParams AzureListAllResources
#'
#' @return Returns Dataframe of Resources
#' @family Resource group functions
#' @export
AzureCreateResourceGroup <- function(AzureActiveContext,ResourceGroup,
                                     Location,SubscriptionID,AzToken,verbose=FALSE) {
  AzureCheckToken(AzureActiveContext)
  if(missing(AzToken)) {ATI <- AzureActiveContext$Token} else (ATI = AzToken)
  if(missing(SubscriptionID)) {SUBIDI <- AzureActiveContext$SubscriptionID} else (SUBIDI = SubscriptionID)
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


#' Delete a ResourceGroup with all Resources.
#'
#' @inheritParams SetAzureContext
#' @inheritParams AzureAuthenticate
#' @inheritParams AzureListAllResources
#'
#' @return Returns Dataframe of Resources
#' @family Resource group functions
#' @export
AzureDeleteResourceGroup <- function(AzureActiveContext,ResourceGroup,SubscriptionID,
                                     AzToken, Type,verbose=FALSE) {
  AzureCheckToken(AzureActiveContext)

  if(missing(AzToken)) {ATI <- AzureActiveContext$Token} else (ATI = AzToken)
  if(missing(SubscriptionID)) {SUBIDI <- AzureActiveContext$SubscriptionID} else (SUBIDI = SubscriptionID)
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
