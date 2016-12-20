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
azureAuthenticate <- function(azureActiveContext, TID, CID, KEY, verbose = FALSE) {

  if (missing(TID)) {
    ATID <- azureActiveContext$TID
  } else (ATID <- TID)
  if (missing(CID)) {
    ACID <- azureActiveContext$CID
  } else (ACID <- CID)
  if (missing(KEY)) {
    AKEY <- azureActiveContext$KEY
  } else (AKEY <- KEY)

  if (!length(ATID)) {
    stop("Error: No TID provided: Use TID argument or set in AzureContext")
  }
  if (!length(ACID)) {
    stop("Error: No CID provided: Use CID argument or set in AzureContext")
  }
  if (!length(AKEY)) {
    stop("Error: No KEY provided: Use KEY argument or set in AzureContext")
  }
  verbosity <- if (verbose)
    httr::verbose(TRUE) else NULL

  URLGT <- paste0("https://login.microsoftonline.com/", ATID, "/oauth2/token?api-version=1.0")

  bodyGT <- paste0("grant_type=client_credentials&resource=https%3A%2F%2Fmanagement.azure.com%2F&client_id=",
                   ACID, "&client_secret=", AKEY)

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
  azureActiveContext$TID    <- ATID
  azureActiveContext$CID    <- ACID
  azureActiveContext$KEY    <- AKEY
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
