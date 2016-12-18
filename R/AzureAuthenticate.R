#' Authenticates against Azure Active Directory application.
#'
#' @inheritParams SetAzureContext
#' @param verbose Print Tracing information (Default False)
#'
#' @note See \url{https://azure.microsoft.com/en-us/documentation/articles/resource-group-create-service-principal-portal/} to learn how to set up an Active Directory application
#' @references \url{https://azure.microsoft.com/en-us/documentation/articles/resource-group-create-service-principal-portal/}
#'
#' @return Retunrs Azure Tokem and sets AzureContext Token
#' @family Resources
#' @export
AzureAuthenticate <- function(AzureActiveContext, TID, CID, KEY, verbose = FALSE) {

  if (missing(TID)) {
    ATID <- AzureActiveContext$TID
  } else (ATID <- TID)
  if (missing(CID)) {
    ACID <- AzureActiveContext$CID
  } else (ACID <- CID)
  if (missing(KEY)) {
    AKEY <- AzureActiveContext$KEY
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
                                 `Content-Type` = "application/x-www-form-urlencoded")),
                  body = bodyGT,
                  verbosity)
  j1 <- content(r, "parsed", encoding = "UTF-8")
  if (status_code(r) != 200) {
    message(j1$error)
    message(j1$error_description)
    stop(paste("Error: Return code", status_code(r)))
  }

  AT <- paste("Bearer", j1$access_token)


  AzureActiveContext$Token  <- AT
  AzureActiveContext$TID    <- ATID
  AzureActiveContext$CID    <- ACID
  AzureActiveContext$KEY    <- AKEY
  AzureActiveContext$EXPIRY <- Sys.time() + 3598
  SUBS <- AzureListSubscriptions(AzureActiveContext)
  return("Authentication Suceeded : Key Obtained")
}



#' Check the timestamp of a Token and Renew if needed.
#'
#' @inheritParams SetAzureContext
#' @inheritParams AzureAuthenticate
#' @family Resources
#' @export
AzureCheckToken <- function(AzureActiveContext) {
  if (is.null(AzureActiveContext$EXPIRY))
    stop("Not Authenticated: Use AzureAuthenticate")

  if (AzureActiveContext$EXPIRY < Sys.time()) {
    message("Azure Token Expired: Attempting automatic renewal")
    AzureAuthenticate(AzureActiveContext)
  }
  return("OK")
}
