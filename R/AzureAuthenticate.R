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
  if (missing(resource)) resource <- azureActiveContext$resource

  assert_that(is_authType(authType))
  assert_that(is_resource(resource))

  print(paste0("Fetch azure active directory access token using authType = ", authType))
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
  assert_that(is_resource(resource))

  verbosity <- set_verbosity(verbose)

  URLGT <- paste0("https://login.microsoftonline.com/", tenantID, "/oauth2/token?api-version=1.0")
  authKeyEncoded <- URLencode(authKey, reserved = TRUE)
  resourceEncoded <- URLencode(resource, reserved = TRUE)
  bodyGT <- paste0("grant_type=client_credentials", "&client_secret=", authKeyEncoded,
                   "&client_id=", clientID, "&resource=", resourceEncoded)
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
#' @references \url{https://azure.microsoft.com/en-us/resources/samples/active-directory-dotnet-deviceprofile/}
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
  assert_that(is_resource(resource))

  verbosity <- set_verbosity(verbose)

  # Before using the preferred auth type, check if access token can be fetched with a valid refresh token. 
  # Any error during this flow should not stop the process. Instead continue with the DeviceCode flow, which 
  # would expect the user to manually authenticate the new device code before generating new 
  # refresh and access token pair.
  refreshToken <- azureActiveContext$RefreshToken
  if (is_refreshToken(refreshToken)) {
    print("Fetch azure active directory access token using RefreshToken")
    result = tryCatch({
      azureGetTokenRefreshToken(azureActiveContext)
    }, error = function(e) {
      print(paste0("Error when fetching azure active directory access token using RefreshToken: ", e))
      azureActiveContext$RefreshToken <- NULL
      return(FALSE)
    })
    if(result) {
      return(TRUE)
    }
  }

  resourceEncoded <- URLencode(resource, reserved = TRUE)
  URLGT <- paste0("https://login.microsoftonline.com/", tenantID, "/oauth2/devicecode?api-version=1.0",
                  "&client_id=", clientID, "&resource=", resourceEncoded)
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
  userCode <- j1$user_code
  deviceCode <- j1$device_code
  verificationURL <- j1$verification_url
  messageToUser <- j1$message
  expiresIn <- j1$expires_in
  pollingInterval <- j1$interval
  
  # display the message to user so that he can take appropriate action
  showDeviceCodeMessageToUser(j1)

  # Wait till user manually approves the user_code in the device code portal
  iteration <- 0
  waiting <- TRUE
  while (iteration < 180 && waiting) {
    Sys.sleep(pollingInterval)
    if(azureGetTokenDeviceCodeFetch(azureActiveContext, tenantID, clientID, deviceCode, resource, verbose)) {
      waiting <- FALSE
    }
    iteration <- iteration + 1
  }

  return(TRUE)
}

#' Display device code flow message to user.
#'
#' @param jsonResponseObject JSON object that contains the message to be displayed.
#'
#' @return If successful, returns TRUE
#' @family Azure resource functions
#'
#' @export
showDeviceCodeMessageToUser <- function(jsonResponseObject) {
  print(jsonResponseObject$message)
  return(TRUE)
}

#' Get Azure token using device_code
#'
#' @inheritParams setAzureContext
#' @param deviceCode Provide the device code obtained in the previous request 
#' @param resource Specify the resource with which the toke is obtained
#' @param verbose Print Tracing information (Default False)
#'
#' @note See \url{https://azure.microsoft.com/en-us/documentation/articles/resource-group-create-service-principal-portal/} for instructions to set up an Active Directory application
#' @references \url{https://azure.microsoft.com/en-us/resources/samples/active-directory-dotnet-deviceprofile/}
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
  assert_that(is_deviceCode(deviceCode))

  verbosity <- set_verbosity(verbose)

  URLGT <- paste0("https://login.microsoftonline.com/", tenantID, "/oauth2/token?api-version=1.0")
  deviceCodeEncoded <- URLencode(deviceCode, reserved = TRUE)
  resourceEncoded <- URLencode(resource, reserved = TRUE)
  bodyGT <- paste0("grant_type=device_code", "&code=", deviceCodeEncoded, "&client_id=", clientID,
                   "&resource=", resourceEncoded)
  r <- httr::POST(URLGT,
                  add_headers(
                    .headers = c(`Cache-Control` = "no-cache",
                                 `Content-type` = "application/x-www-form-urlencoded")),
                  body = bodyGT,
                  verbosity)
  # handle special error - HTTP400 - authorization_pending
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
#' @param refreshToken Provide the previously obtained refreshToken
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
  assert_that(is_refreshToken(refreshToken))

  verbosity <- set_verbosity(verbose)

  URLGT <- paste0("https://login.microsoftonline.com/", tenantID, "/oauth2/token?api-version=1.0")
  refreshTokenEncoded <- URLencode(refreshToken, reserved = TRUE)
  # NOTE: Providing the optional client ID fails the request!
  bodyGT <- paste0("grant_type=refresh_token", "&refresh_token=", refreshTokenEncoded)
  r <- httr::POST(URLGT,
                  add_headers(
                    .headers = c(`Cache-Control` = "no-cache",
                                 `Content-type` = "application/x-www-form-urlencoded")),
                  body = bodyGT,
                  verbosity)
  stopWithAzureError(r)

  j1 <- content(r, "parsed", encoding = "UTF-8")
  azToken <- paste("Bearer", j1$access_token)
  azRefreshToken <- j1$refresh_token

  azureActiveContext$Token  <- azToken
  azureActiveContext$tenantID    <- tenantID
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

