
#' Get available Subscriptions.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#'
#' @return Returns Dataframe of subscriptionID sets AzureContext subscriptionID
#' @family Resource group functions
#' @export
azureListSubscriptions <- function(azureActiveContext, azToken, verbose = FALSE) {
  if (missing(azToken)) {
    AT <- azureActiveContext$Token
  } else (AT = azToken)
  if (nchar(AT) < 5) {
    stop("Error: No Token / Not currently Authenticated.")
  }
  verbosity <- if (verbose)
    httr::verbose(TRUE) else NULL

  URLGS <- "https://management.azure.com/subscriptions?api-version=2015-01-01"

  r <- GET(URLGS,
           add_headers(.headers = c(Host = "management.azure.com",
                                    Authorization = AT,
                                    `Content-type` = "application/json")),
           verbosity)
  dfs <- lapply(content(r), data.frame, stringsAsFactors = FALSE)
  # head(dfs)
  colnames(dfs)
  typeof(dfs)
  df1 <- rbind.fill(dfs)
  if (nrow(df1) == 1)
    azureActiveContext$subscriptionID <- df1[, 2]
  return(df1)
}


#' Get all resource groups in subscription ID.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#'
#' @return Returns Dataframe of resourceGroups
#' @family Resource group functions
#' @export
azureListRG <- function(azureActiveContext, subscriptionID, azToken, verbose = FALSE) {
  azureCheckToken(azureActiveContext)
  if (missing(azToken)) {
    ATI <- azureActiveContext$Token
  } else (ATI = azToken)
  if (missing(subscriptionID)) {
    SUBIDI <- azureActiveContext$subscriptionID
  } else (SUBIDI = subscriptionID)
  if (!length(ATI)) {
    stop("Error: No Token / Not currently Authenticated.")
  }
  if (!length(SUBIDI)) {
    stop("Error: No subscriptionID provided: Use SUBID argument or set in AzureContext")
  }
  verbosity <- if (verbose)
    httr::verbose(TRUE) else NULL

  URLRG <- paste("https://management.azure.com/subscriptions/", SUBIDI,
                 "/resourcegroups?api-version=2015-01-01", sep = "")

  r <- GET(URLRG, add_headers(.headers = c(Host = "management.azure.com",
                                           Authorization = ATI,
                                           `Content-type` = "application/json")),
           verbosity)
  if (status_code(r) != 200) stopWithAzureError(r)

  rl <- content(r, "text", encoding = "UTF-8")
  df <- fromJSON(rl)
  dfn <- df$value[, c("name", "location", "id")]
  names(dfn) <- c("name", "location", "ID")
  dfn$resourceGroup <- extractResourceGroupname(dfn$ID)

  return(dfn)
}


#' @rdname azureListRG
#' @export
AzureListRG <- azureListRG


#' Get all Resource in default Subscription.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#'
#' @param azureActiveContext Azure Context Object
#' @param resourceGroup resourceGroup Object (or use azureActiveContext)
#' @param subscriptionID subscriptionID Object (or use azureActiveContext)
#' @param name filter by resource name
#' @param type filter by resource type
#' @param location Azure region, e.g. 'westeurope' or 'southcentralus'
#'
#' @return Returns Dataframe of Resources
#' @family Resource group functions
#' @export
azureListAllResources <- function(azureActiveContext, resourceGroup, subscriptionID,
                                  azToken, name, type, location, verbose = FALSE) {

  azureCheckToken(azureActiveContext)

  if (missing(azToken)) {
    ATI <- azureActiveContext$Token
  } else (ATI = azToken)
  if (missing(subscriptionID)) {
    SUBIDI <- azureActiveContext$subscriptionID
  } else (SUBIDI = subscriptionID)
  # if (ATI == '') stop('Token not provided') if (SUBIDI == '')
  # stop('Subscription not provided')

  if (!length(ATI)) {
    stop("Error: No Token / Not currently Authenticated.")
  }
  if (!length(SUBIDI)) {
    stop("Error: No subscriptionID provided: Use SUBID argument or set in AzureContext")
  }
  verbosity <- if (verbose)
    httr::verbose(TRUE) else NULL

  URLRS <- paste("https://management.azure.com/subscriptions/", SUBIDI,
                 "/resources?api-version=2015-01-01", sep = "")
  r <- GET(URLRS, add_headers(.headers = c(Host = "management.azure.com",
                                           Authorization = ATI,
                                           `Content-type` = "application/json")),
           verbosity)
  rl <- content(r, "text", encoding = "UTF-8")
  df <- fromJSON(rl)
  #print(df)
  dfn <- df$value[, c("name", "type", "location", "id")]

  dfn$resourceGroup  <- extractResourceGroupname(dfn$id)
  dfn$RG             <- dfn$resourceGroup
  dfn$subscriptionID <- extractSubscriptionID(dfn$id)


  if (!missing(name))          dfn <- dfn[grep(name, dfn$name), ]
  if (!missing(type))          dfn <- dfn[grep(type, dfn$type), ]
  if (!missing(location))      dfn <- dfn[grep(location, dfn$location), ]
  if (!missing(resourceGroup) && !is.null(resourceGroup)) {
    dfn <- dfn[grep(resourceGroup, dfn$resourceGroup), ]
  }

  dfn[, c(1:2, 6, 3:5, 7)]
}


#' Create a resourceGroup.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @inheritParams azureListAllResources
#'
#' @return Returns Dataframe of Resources
#' @family Resource group functions
#' @export
azureCreateResourceGroup <- function(azureActiveContext, resourceGroup,
                                     location, subscriptionID, azToken, verbose = FALSE) {
  azureCheckToken(azureActiveContext)
  if (missing(azToken)) {
    ATI <- azureActiveContext$Token
  } else (ATI = azToken)
  if (missing(subscriptionID)) {
    SUBIDI <- azureActiveContext$subscriptionID
  } else (SUBIDI = subscriptionID)
  # if (ATI == '') stop('Token not provided') if (SUBIDI == '')
  # stop('Subscription not provided')
  if (missing(resourceGroup)) {
    RGI <- azureActiveContext$resourceGroup
  } else {
    RGI <- resourceGroup
    azureActiveContext$resourceGroup <- RGI
  }
  if (missing(location)) {
    stop("Error: No location provided")
  }

  verbosity <- if (verbose)
    httr::verbose(TRUE) else NULL

  if (!length(RGI)) {
    stop("Error: No resourceGroup provided: Use resourceGroup argument or set in AzureContext")
  }
  if (!length(ATI)) {
    stop("Error: No Token / Not currently Authenticated.")
  }
  if (!length(SUBIDI)) {
    stop("Error: No subscriptionID provided: Use SUBID argument or set in AzureContext")
  }

  bodyI <- list(location = location)

  URLRS <- paste("https://management.azure.com/subscriptions/", SUBIDI,
                 "/resourcegroups/", RGI, "?api-version=2015-01-01", sep = "")
  r <- PUT(URLRS, add_headers(.headers = c(Host = "management.azure.com",
                                           Authorization = ATI,
                                           `Content-type` = "application/json")),
           body = bodyI,
           encode = "json",
           verbosity)
  if (!status_code(r) %in% c(200, 201)) stopWithAzureError(r)

  rl <- content(r, "text", encoding = "UTF-8")
  df <- fromJSON(rl)
  if (length(df$error$code) && df$error$code == "locationNotAvailableForresourceGroup")
    stop(df$message)

  return("Create Request Submitted")
}


#' Delete a resourceGroup with all Resources.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @inheritParams azureListAllResources
#'
#' @return Returns Dataframe of Resources
#' @family Resource group functions
#' @export
azureDeleteResourceGroup <- function(azureActiveContext, resourceGroup,
                                     subscriptionID, azToken, type, verbose = FALSE) {
  azureCheckToken(azureActiveContext)

  if (missing(azToken)) {
    ATI <- azureActiveContext$Token
  } else (ATI = azToken)
  if (missing(subscriptionID)) {
    SUBIDI <- azureActiveContext$subscriptionID
  } else (SUBIDI = subscriptionID)
  # if (ATI == '') stop('Token not provided') if (SUBIDI == '')
  # stop('Subscription not provided')
  if (missing(resourceGroup))
    stop("Please supply Resource Group to Confirm")
  if (missing(resourceGroup)) {
    RGI <- azureActiveContext$resourceGroup
  } else (RGI = resourceGroup)
  verbosity <- if (verbose)
    httr::verbose(TRUE) else NULL

  if (!length(RGI)) {
    stop("Error: No resourceGroup provided: Use resourceGroup argument or set in AzureContext")
  }
  if (!length(ATI)) {
    stop("Error: No Token / Not currently Authenticated.")
  }
  if (!length(SUBIDI)) {
    stop("Error: No subscriptionID provided: Use SUBID argument or set in AzureContext")
  }

  URLRS <- paste("https://management.azure.com/subscriptions/", SUBIDI,
                 "/resourcegroups/", RGI, "?api-version=2015-01-01", sep = "")
  r <- DELETE(URLRS, add_headers(.headers = c(Host = "management.azure.com",
                                              Authorization = ATI,
                                              `Content-type` = "application/json")),
              verbosity)
  if (status_code(r) == 404) {
    stop(paste0("Error: Resource Group Not Found(", status_code(r), ")"))
  }
  if (status_code(r) != 202) {
    stop(paste0("Error: Return code(", status_code(r), ")"))
  }
  rl <- content(r, "text", encoding = "UTF-8")
  if (nchar(rl) > 1) {
    df <- fromJSON(rl)
    stop(df$error$message)
  }
  return("Delete Request Submitted")
}
