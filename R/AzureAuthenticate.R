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

  # before using the preferred auth type, check if access token can be fetched with a valid refresh token
  refreshToken <- azureActiveContext$RefreshToken
  if (!is.null(refreshToken) && nchar(refreshToken) > 0) {
    authType <- "RefreshToken"
  }

  result <- switch(
    authType,
    RefreshToken = azureGetTokenRefreshToken(azureActiveContext),
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

#' Get Azure token using client credentials
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
  # TODO: check resource?

  verbosity <- set_verbosity(verbose)

  URLGT <- paste0("https://login.microsoftonline.com/", tenantID, "/oauth2/token?api-version=1.0")

  authKeyEncoded <- URLencode(authKey, reserved = TRUE)
  resourceEncoded <- URLencode(resource, reserved = TRUE)
  bodyGT <- paste0("grant_type=client_credentials", "&client_id=", clientID, "&client_secret=", authKeyEncoded,
                   "&resource=", resourceEncoded)

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

#' Get Azure token using DeviceCode.
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

  # display the message to user so that he can take appropriate action
  showDeviceCodeMessageToUser(j1)

  userCode <- j1$user_code
  deviceCode <- j1$device_code
  verificationURL <- j1$verification_url
  messageToUser <- j1$message
  expiresIn <- j1$expires_in
  pollingInterval <- j1$interval

  iteration <- 0
  waiting <- TRUE
  while (iteration < 50 && waiting) {
    Sys.sleep(pollingInterval)
    if(azureGetTokenDeviceCodeFetch(azureActiveContext, tenantID, clientID, deviceCode, resource, verbose)) {
      waiting <- FALSE
    }
    iteration <- iteration + 1
  }

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
  if (missing(deviceCode)) deviceCode <- azureActiveContext$deviceCode
  if (missing(resource)) resource <- azureActiveContext$resource

  assert_that(is_tenant_id(tenantID))
  assert_that(is_client_id(clientID))
  # TODO: check device code?

  verbosity <- set_verbosity(verbose)

  URLGT <- paste0("https://login.microsoftonline.com/", tenantID, "/oauth2/token?api-version=1.0")
  deviceCodeEncoded <- URLencode(deviceCode, reserved = TRUE)
  resourceEncoded <- URLencode(resource, reserved = TRUE)
  bodyGT <- paste0("grant_type=device_code&code=", deviceCodeEncoded,
                   "&client_id=", clientID, "&resource=", resourceEncoded)

  r <- httr::POST(URLGT,
                  add_headers(
                    .headers = c(`Cache-Control` = "no-cache",
                                 `Content-type` = "application/x-www-form-urlencoded")),
                  body = bodyGT,
                  verbosity)
  # handle special error case
  if(status_code(r) == 400) {
    rr <- content(r)
    if (rr$error == "authorization_pending") {
      print("polled AAD for token, got authorization_pending (still waiting for user to complete login)")
      return(FALSE)
    }
  }
  stopWithAzureError(r)

  j1 <- content(r, "parsed", encoding = "UTF-8")

  azToken <- paste("Bearer", j1$access_token)
  azRefreshToken <- j1$refresh_token

  azureActiveContext$Token  <- azToken
  azureActiveContext$tenantID    <- tenantID
  azureActiveContext$clientID    <- clientID
  azureActiveContext$EXPIRY <- Sys.time() + 3598
  azureActiveContext$resource <- resource
  azureActiveContext$RefreshToken <- azRefreshToken
  return(TRUE)
}

#' Get Azure token using RefreshToken
#'
#' @inheritParams setAzureContext
#' @param resource Specify the resource with which the token is obtained
#' @param verbose Print Tracing information (Default False)
#'
#' @note See \url{https://azure.microsoft.com/en-us/documentation/articles/resource-group-create-service-principal-portal/} for instructions to set up an Active Directory application
#' @references \url{https://azure.microsoft.com/en-us/documentation/articles/resource-group-create-service-principal-portal/}
#'
#' @return If successful, returns TRUE
#' @family Azure resource functions
#'
#' @export
azureGetTokenRefreshToken <- function(azureActiveContext, tenantID, refreshToken, verbose = FALSE) {
  assert_that(is.azureActiveContext(azureActiveContext))

  if (missing(tenantID)) tenantID <- azureActiveContext$tenantID
  if (missing(refreshToken)) refreshToken <- azureActiveContext$RefreshToken

  assert_that(is_tenant_id(tenantID))
  assert_that(is_client_id(clientID))
  # TODO: need another validate function
  #assert_that(is_authKey(refreshToken))

  verbosity <- set_verbosity(verbose)

  URLGT <- paste0("https://login.microsoftonline.com/", tenantID, "/oauth2/token?api-version=1.0")
  refreshTokenEncoded <- URLencode(refreshToken, reserved = TRUE)
  bodyGT <- paste0("grant_type=refresh_token&refresh_token=", refreshTokenEncoded)

  r <- httr::POST(URLGT,
                  add_headers(
                    .headers = c(`Content-type` = "application/x-www-form-urlencoded")),
                  body = bodyGT,
                  verbosity)
  stopWithAzureError(r)

  j1 <- content(r, "parsed", encoding = "UTF-8")

  azToken <- paste("Bearer", j1$access_token)
  azRefreshToken <- j1$refresh_token

  azureActiveContext$Token  <- azToken
  azureActiveContext$tenantID    <- tenantID
  azureActiveContext$clientID    <- clientID
  azureActiveContext$EXPIRY <- Sys.time() + 3598
  azureActiveContext$RefreshToken <- azRefreshToken
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

