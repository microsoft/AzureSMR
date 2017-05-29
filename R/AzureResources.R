
#' Get available subscriptions.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#'
#' @return Dataframe of subscriptionID; Sets AzureContext subscriptionID
#' @family Resource group functions
#' @export
azureListSubscriptions <- function(azureActiveContext, verbose = FALSE) {
  assert_that(is.azureActiveContext(azureActiveContext))
  azureCheckToken(azureActiveContext)
  azToken <- azureActiveContext$Token
  verbosity <- set_verbosity(verbose)

  URLGS <- "https://management.azure.com/subscriptions?api-version=2015-01-01"
  r <- GET(URLGS, azureApiHeaders(azToken), verbosity)
  stopWithAzureError(r)

  dfs <- lapply(content(r), data.frame, stringsAsFactors = FALSE)
  df1 <- do.call(rbind, dfs))
  if (nrow(df1) == 1) azureActiveContext$subscriptionID <- df1$subscriptionID[1]
  return(df1)
}


#' Get all resource groups in subscription ID.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#'
#' @return Dataframe of resourceGroups
#' @family Resource group functions
#' @export
azureListRG <- function(azureActiveContext, subscriptionID, verbose = FALSE) {
  assert_that(is.azureActiveContext(azureActiveContext))
  azureCheckToken(azureActiveContext)
  azToken <- azureActiveContext$Token
  verbosity <- set_verbosity(verbose)

  if (missing(subscriptionID)) subscriptionID <- azureActiveContext$subscriptionID
  assert_that(is_subscription_id(subscriptionID))

  URLRG <- paste0("https://management.azure.com/subscriptions/", subscriptionID,
                 "/resourcegroups?api-version=2015-01-01")

  r <- GET(URLRG, azureApiHeaders(azToken), verbosity)
  stopWithAzureError(r)

  rl <- content(r, "text", encoding = "UTF-8")
  df <- fromJSON(rl)
  dfn <- df$value[, c("name", "location", "id")]
  names(dfn) <- c("name", "location", "ID")
  dfn$resourceGroup <- extractResourceGroupname(dfn$ID)
  return(dfn)
}

getResourceGroupLocation <- function(azureActiveContext, resourceGroup) {
  assert_that(is.azureActiveContext(azureActiveContext))
  assert_that(is_resource_group(resourceGroup))
  z <- azureListRG(azureActiveContext)
  z[z[["name"]] == resourceGroup, "location"]
}

#' Get all Resource in default Subscription.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#'
#' @param azureActiveContext Azure Context Object
#' @param resourceGroup resourceGroup Object (or use `azureActiveContext`)
#' @param subscriptionID subscriptionID Object (or use `azureActiveContext`)
#' @param name filter by resource name
#' @param type filter by resource type
#' @param location Azure region, e.g. 'westeurope' or 'southcentralus'
#'
#' @return Returns Dataframe of Resources
#' @family Resource group functions
#' @export
azureListAllResources <- function(azureActiveContext, resourceGroup, subscriptionID,
                                  name, type, location, verbose = FALSE) {

  assert_that(is.azureActiveContext(azureActiveContext))
  azureCheckToken(azureActiveContext)
  azToken <- azureActiveContext$Token
  if (missing(subscriptionID)) subscriptionID <- azureActiveContext$subscriptionID
  verbosity <- set_verbosity(verbose)


  url <- paste0("https://management.azure.com/subscriptions/", subscriptionID,
                 "/resources?api-version=2015-01-01")
  r <- GET(url, azureApiHeaders(azToken), verbosity)
  stopWithAzureError(r)

  rl <- content(r, "text", encoding = "UTF-8")
  df <- fromJSON(rl)
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
                                     location, subscriptionID, verbose = FALSE) {
  assert_that(is.azureActiveContext(azureActiveContext))
  azureCheckToken(azureActiveContext)
  azToken <- azureActiveContext$Token
  if (missing(subscriptionID)) subscriptionID <- azureActiveContext$subscriptionID
  if (missing(resourceGroup)) resourceGroup <- azureActiveContext$resourceGroup
  verbosity <- set_verbosity(verbose)
  assert_that(is_resource_group(resourceGroup))
  assert_that(is_subscription_id(subscriptionID))
  assert_that(is_location(location))
  
  azureActiveContext$resourceGroup <- resourceGroup
  verbosity <- set_verbosity(verbose)

  bodyI <- list(location = location)

  url <- paste0("https://management.azure.com/subscriptions/", subscriptionID,
                 "/resourcegroups/", resourceGroup, "?api-version=2015-01-01")
  r <- httr::PUT(url, azureApiHeaders(azToken), body = bodyI, encode = "json", verbosity)
  stopWithAzureError(r)

  rl <- content(r, "text", encoding = "UTF-8")
  df <- fromJSON(rl)
  if (length(df$error$code) && df$error$code == "locationNotAvailableForresourceGroup")
    stop(df$message)

  message("Create Request Submitted")
  return(TRUE)
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
                                     subscriptionID, type, verbose = FALSE) {
  assert_that(is.azureActiveContext(azureActiveContext))
  azureCheckToken(azureActiveContext)
  azToken <- azureActiveContext$Token

  if (missing(subscriptionID)) subscriptionID <- azureActiveContext$subscriptionID
  verbosity <- set_verbosity(verbose)

  assert_that(is_resource_group(resourceGroup))
  assert_that(is_subscription_id(subscriptionID))

  url <- paste0("https://management.azure.com/subscriptions/", subscriptionID,
                 "/resourcegroups/", resourceGroup, "?api-version=2015-01-01")
  r <- DELETE(url, azureApiHeaders(azToken), verbosity)
  if (status_code(r) == 404) {
    stop(paste0("Error: Resource Group Not Found(", status_code(r), ")"))
  }
  stopWithAzureError(r)
  message("Delete Request Submitted")
  return(TRUE)
}
