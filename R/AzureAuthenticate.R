#' Authenticates against Azure Active directory application.
#'
#' @inheritParams setAzureContext
#' @param verbose If TRUE, prints verbose messages
#' @param resource URL of azure management portal
#'
#' @note See \url{https://azure.microsoft.com/en-us/documentation/articles/resource-group-create-service-principal-portal/} for instructions to set up an Active Directory application
#' @references \url{https://azure.microsoft.com/en-us/documentation/articles/resource-group-create-service-principal-portal/}
#'
#' @return If successful, returns TRUE
#' @family Azure resource functions
#' 
#' @export
azureAuthenticate <- function(azureActiveContext, tenantID, clientID, authKey, 
                              verbose = FALSE, 
                              resource = "https://management.azure.com/") {
  assert_that(is.azureActiveContext(azureActiveContext))

  if (missing(tenantID)) tenantID <- azureActiveContext$tenantID
  if (missing(clientID)) clientID <- azureActiveContext$clientID 
  if (missing(authKey)) authKey <- azureActiveContext$authKey
  
  resource <- URLencode(resource, reserved = TRUE, repeated = TRUE)

  assert_that(is_tenant_id(tenantID))
  assert_that(is_client_id(clientID))
  assert_that(is_authKey(authKey))
  verbosity <- set_verbosity(verbose)


  URLGT <- paste0("https://login.microsoftonline.com/", tenantID, "/oauth2/token?api-version=1.0")

  authKeyEncoded <- URLencode(authKey, reserved = TRUE)

  bodyGT <- paste0("grant_type=client_credentials&resource=", resource, "&client_id=",
                   clientID, "&client_secret=", authKeyEncoded)

  r <- httr::POST(URLGT,
                  add_headers(
                    .headers = c(`Cache-Control` = "no-cache",
                                 `Content-type` = "application/x-www-form-urlencoded")),
                  body = bodyGT,
                  verbosity)
  stopWithAzureError(r)
  
  j1 <- content(r, "parsed", encoding = "UTF-8")

  azToken <- paste("Bearer", j1$access_token)

  azureActiveContext$Token  <- azToken
  azureActiveContext$tenantID    <- tenantID
  azureActiveContext$clientID    <- clientID
  azureActiveContext$authKey    <- authKey
  azureActiveContext$EXPIRY <- Sys.time() + 3598
  azureListSubscriptions(azureActiveContext) # this sets the subscription ID
  #if(verbose) message("Authentication succeeded: key obtained")
  return(TRUE)
}



#' Check the timestamp of a token and renew if needed.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @family Azure resource functions
#' @export
azureCheckToken <- function(azureActiveContext) {
  if (missing(azureActiveContext) || is.null(azureActiveContext)) return(FALSE)
  if (is.null(azureActiveContext$EXPIRY))
    stop("Not authenticated: Use azureAuthenticate()")

  if (azureActiveContext$EXPIRY < Sys.time()) {
    message("Azure token expired: attempting automatic renewal")
    azureAuthenticate(azureActiveContext)
  }
  return(TRUE)
}
