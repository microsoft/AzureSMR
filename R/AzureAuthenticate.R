#' Authenticates against Azure Active directory application.
#'
#' @inheritParams setAzureContext
#' @param verbose Print Tracing information (Default False)
#'
#' @note See \url{https://azure.microsoft.com/en-us/documentation/articles/resource-group-create-service-principal-portal/} for instructions to set up an Active Directory application
#' @references \url{https://azure.microsoft.com/en-us/documentation/articles/resource-group-create-service-principal-portal/}
#'
#' @return If successful, returns TRUE
#' @family Azure resource functions
#' 
#' @export
azureAuthenticate <- function(azureActiveContext, tenantID, clientID, authKey, authType = "ClientCredential", resource = "https://management.azure.com/", verbose = FALSE) {
  assert_that(is.azureActiveContext(azureActiveContext))

  azureAuthenticateOnAuthType(azureActiveContext, authType = authType, resource = resource, verbose = verbose)

  azureListSubscriptions(azureActiveContext) # this sets the subscription ID
  #if(verbose) message("Authentication succeeded: key obtained")
  return(TRUE)
}

#' Switch based on auth types.
#'
#' @inheritParams setAzureContext
#' @param verbose Print Tracing information (Default False)
#'
#' @return If successful, returns TRUE
#' @family Azure resource functions
#'
#' @export
azureAuthenticateOnAuthType <- function(azureActiveContext, authType, resource, verbose = FALSE) {
  assert_that(is.azureActiveContext(azureActiveContext))

  if (missing(authType)) authType <- azureActiveContext$authType
  if (is.null(authType) || nchar(authType) == 0)
    stop("Unspecified Auth Type. Please specify a valid authType.")

  if (missing(resource)) resource <- azureActiveContext$resource
  if (is.null(resource) || nchar(resource) == 0)
    stop("Unspecified Resource. Please specify a valid resource.")

  result <- switch(
    authType,
    ClientCredential = azureGetTokenClientCredential(azureActiveContext, resource = resource, verbose = verbose),
    DeviceCode = azureGetTokenDeviceCode(azureActiveContext, resource = resource, verbose = verbose),
    FALSE
  )

  # persist valid auth type in the context
  if (result) {
    azureActiveContext$authType <- authType
  }

  return(result)
}

#' Get Azure token
#'
#' @inheritParams setAzureContext
#' @param resource Specify the resource with which the toke is obtained
#' @param verbose Print Tracing information (Default False)
#'
#' @note See \url{https://azure.microsoft.com/en-us/documentation/articles/resource-group-create-service-principal-portal/} for instructions to set up an Active Directory application
#' @references \url{https://azure.microsoft.com/en-us/documentation/articles/resource-group-create-service-principal-portal/}
#'
#' @return If successful, returns TRUE
#' @family Azure resource functions
#'
#' @export
azureGetTokenClientCredential <- function(azureActiveContext, tenantID, clientID, authKey, resource, verbose = FALSE) {
  assert_that(is.azureActiveContext(azureActiveContext))

  if (missing(tenantID)) tenantID <- azureActiveContext$tenantID
  if (missing(clientID)) clientID <- azureActiveContext$clientID 
  if (missing(authKey)) authKey <- azureActiveContext$authKey
  if (missing(resource)) resource <- azureActiveContext$resource

  assert_that(is_tenant_id(tenantID))
  assert_that(is_client_id(clientID))
  assert_that(is_authKey(authKey))
  verbosity <- set_verbosity(verbose)

  URLGT <- paste0("https://login.microsoftonline.com/", tenantID, "/oauth2/token?api-version=1.0")

  authKeyEncoded <- URLencode(authKey, reserved = TRUE)
  resourceEncoded <- URLencode(resource, reserved = TRUE)
  bodyGT <- paste0("grant_type=client_credentials&resource=", resourceEncoded, "&client_id=",
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
  azureActiveContext$resource <- resource
  return(TRUE)
}

#' Authenticates against Azure Active directory application using DeviceCode flow.
#'
#' @inheritParams setAzureContext
#' @param verbose Print Tracing information (Default False)
#'
#' @note See \url{https://azure.microsoft.com/en-us/documentation/articles/resource-group-create-service-principal-portal/} for instructions to set up an Active Directory application
#' @references \url{https://azure.microsoft.com/en-us/documentation/articles/resource-group-create-service-principal-portal/}
#'
#' @return If successful, returns TRUE
#' @family Azure resource functions
#'
#' @export
azureGetTokenDeviceCode <- function(azureActiveContext, tenantID, clientID, resource, verbose = FALSE) {
  assert_that(is.azureActiveContext(azureActiveContext))

  if (missing(tenantID)) tenantID <- azureActiveContext$tenantID
  if (missing(clientID)) clientID <- "2d302a05-86f0-4e0a-a8f6-ae4d28a035df"
  if (missing(resource)) resource <- azureActiveContext$resource

  assert_that(is_tenant_id(tenantID))
  assert_that(is_client_id(clientID))

  verbosity <- set_verbosity(verbose)

  resourceEncoded <- URLencode(resource, reserved = TRUE)
  URLGT <- paste0(
    "https://login.microsoftonline.com/", tenantID,
    "/oauth2/devicecode?resource=", resourceEncoded,
    "&client_id=", clientID
  )

  r <- httr::GET(
    URLGT,
    add_headers(
      .headers = c(
        `Cache-Control` = "no-cache", 
        `Content-type` = "application/x-www-form-urlencoded")
    ),
    verbosity
  )
  stopWithAzureError(r)

  j1 <- content(r, "parsed", encoding = "UTF-8")
  print(class(j1))

  # display the message to user so that he can take appropriate action
  showDeviceCodeMessageToUser(j1)

  deviceCode <- j1$device_code

#  wait_for_azure(
#    sa_name %in% azureListSA(asc)$storageAccount
#  )

  return(TRUE)
}

showDeviceCodeMessageToUser <- function(jsonResponseObject) {
  print(jsonResponseObject$message)
}

#' Get Azure token using device_code
#'
#' @inheritParams setAzureContext
#' @param resource Specify the resource with which the toke is obtained
#' @param verbose Print Tracing information (Default False)
#'
#' @note See \url{https://azure.microsoft.com/en-us/documentation/articles/resource-group-create-service-principal-portal/} for instructions to set up an Active Directory application
#' @references \url{https://azure.microsoft.com/en-us/documentation/articles/resource-group-create-service-principal-portal/}
#'
#' @return If successful, returns TRUE
#' @family Azure resource functions
#'
#' @export
azureGetTokenDeviceCodeFetch <- function(azureActiveContext, tenantID, clientID, deviceCode, resource, verbose = FALSE) {
  assert_that(is.azureActiveContext(azureActiveContext))

  if (missing(tenantID)) tenantID <- azureActiveContext$tenantID
  if (missing(clientID)) clientID <- azureActiveContext$clientID 
  if (missing(deviceCode)) deviceCode <- azureActiveContext$authKey
  if (missing(resource)) resource <- azureActiveContext$resource

  assert_that(is_tenant_id(tenantID))
  assert_that(is_client_id(clientID))

  verbosity <- set_verbosity(verbose)

  URLGT <- paste0("https://login.microsoftonline.com/", tenantID, "/oauth2/token?api-version=1.0")

  authKeyEncoded <- URLencode(authKey, reserved = TRUE)
  resourceEncoded <- URLencode(resource, reserved = TRUE)
  bodyGT <- paste0("grant_type=client_credentials&resource=", resourceEncoded, "&client_id=",
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
  azureActiveContext$resource <- resource
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
    azureAuthenticateOnAuthType(azureActiveContext)
  }
  return(TRUE)
}

