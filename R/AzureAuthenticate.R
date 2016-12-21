#' Authenticates against Azure Active directory application.
#'
#' @inheritParams setAzureContext
#' @param verbose Print Tracing information (Default False)
#'
#' @note See \url{https://azure.microsoft.com/en-us/documentation/articles/resource-group-create-service-principal-portal/} to learn how to set up an Active directory application
#' @references \url{https://azure.microsoft.com/en-us/documentation/articles/resource-group-create-service-principal-portal/}
#'
#' @return Retunrs Azure Tokem and sets AzureContext Token
#' @family Resources
#' @export
azureAuthenticate <- function(azureActiveContext, tenantID, clientID, authKey, verbose = FALSE) {

  if (missing(tenantID)) {
    AtenantID <- azureActiveContext$tenantID
  } else (AtenantID <- tenantID)
  if (missing(clientID)) {
    AclientID <- azureActiveContext$clientID
  } else (AclientID <- clientID)
  if (missing(authKey)) {
    AauthKey <- azureActiveContext$authKey
  } else (AauthKey <- authKey)

  if (!length(AtenantID)) {
    stop("Error: No tenantID provided: Use tenantID argument or set in AzureContext")
  }
  if (!length(AclientID)) {
    stop("Error: No clientID provided: Use clientID argument or set in AzureContext")
  }
  if (!length(AauthKey)) {
    stop("Error: No authKey provided: Use authKey argument or set in AzureContext")
  }
  verbosity <- if (verbose)
    httr::verbose(TRUE) else NULL

  URLGT <- paste0("https://login.microsoftonline.com/", AtenantID, "/oauth2/token?api-version=1.0")

  bodyGT <- paste0("grant_type=client_credentials&resource=https%3A%2F%2Fmanagement.azure.com%2F&client_id=",
                   AclientID, "&client_secret=", AauthKey)

  r <- httr::POST(URLGT,
                  add_headers(
                    .headers = c(`Cache-Control` = "no-cache",
                                 `Content-type` = "application/x-www-form-urlencoded")),
                  body = bodyGT,
                  verbosity)
  j1 <- content(r, "parsed", encoding = "UTF-8")
  if (status_code(r) != 200) stopWithAzureError(r)

  AT <- paste("Bearer", j1$access_token)


  azureActiveContext$Token  <- AT
  azureActiveContext$tenantID    <- AtenantID
  azureActiveContext$clientID    <- AclientID
  azureActiveContext$authKey    <- AauthKey
  azureActiveContext$EXPIRY <- Sys.time() + 3598
  SUBS <- azureListSubscriptions(azureActiveContext)
  return("Authentication Suceeded : Key Obtained")
}



#' Check the timestamp of a Token and Renew if needed.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @family Resources
#' @export
azureCheckToken <- function(azureActiveContext) {
  if (is.null(azureActiveContext$EXPIRY))
    stop("Not Authenticated: Use azureAuthenticate")

  if (azureActiveContext$EXPIRY < Sys.time()) {
    message("Azure Token Expired: Attempting automatic renewal")
    azureAuthenticate(azureActiveContext)
  }
  return("OK")
}
