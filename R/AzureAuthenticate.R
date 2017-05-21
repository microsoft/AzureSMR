#' Authenticates against Azure Active directory application.
#'
#' @inheritParams setAzureContext
#' @param verbose Print Tracing information (Default False)
#'
#' @note See \url{https://azure.microsoft.com/en-us/documentation/articles/resource-group-create-service-principal-portal/} for instructions to set up an Active Directory application
#' @references \url{https://azure.microsoft.com/en-us/documentation/articles/resource-group-create-service-principal-portal/}
#'
#' @return Returns Azure token and sets AzureContext token
#' @family Azure resource functions
#' 
#' @importFrom utils URLencode
#' @export
azureAuthenticate <- function(azureActiveContext, tenantID, clientID, authKey, verbose = FALSE) {

  if (missing(tenantID)) {
    tenantID <- azureActiveContext$tenantID
  } else (tenantID <- tenantID)
  if (missing(clientID)) {
    clientID <- azureActiveContext$clientID
  } else (clientID <- clientID)
  if (missing(authKey)) {
    authKey <- azureActiveContext$authKey
  } else (authKey <- authKey)

  if (!length(tenantID)) {
    stop("Error: No tenantID provided: Use tenantID argument or set in AzureContext")
  }
  if (!length(clientID)) {
    stop("Error: No clientID provided: Use clientID argument or set in AzureContext")
  }
  if (!length(authKey)) {
    stop("Error: No authKey provided: Use authKey argument or set in AzureContext")
  }
  verbosity <- if (verbose)
    httr::verbose(TRUE) else NULL

  URLGT <- paste0("https://login.microsoftonline.com/", tenantID, "/oauth2/token?api-version=1.0")

  authKeyEncoded <- URLencode(authKey, reserved = TRUE)

  bodyGT <- paste0("grant_type=client_credentials&resource=https%3A%2F%2Fmanagement.azure.com%2F&client_id=",
                   clientID, "&client_secret=", authKeyEncoded)

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
  azureActiveContext$tenantID    <- tenantID
  azureActiveContext$clientID    <- clientID
  azureActiveContext$authKey    <- authKey
  azureActiveContext$EXPIRY <- Sys.time() + 3598
  SUBS <- azureListSubscriptions(azureActiveContext)
  message("Authentication Suceeded : Key Obtained")
  return(TRUE)
}



#' Check the timestamp of a token and renew if needed.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @family Azure resource functions
#' @export
azureCheckToken <- function(azureActiveContext) {
  if (missing(azureActiveContext) || is.null(azureActiveContext)) return(NA)
  if (is.null(azureActiveContext$EXPIRY))
    stop("Not Authenticated: Use azureAuthenticate")

  if (azureActiveContext$EXPIRY < Sys.time()) {
    message("Azure Token Expired: Attempting automatic renewal")
    azureAuthenticate(azureActiveContext)
  }
  return(TRUE)
}
