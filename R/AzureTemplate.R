#' Deploy a resource from an Azure Resource Manager (ARM) template.
#'
#' Deploy a resource from a template. See https://github.com/Azure/azure-quickstart-templates
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @inheritParams azureResizeHDI
#'
#' @param deplname Deployment name
#' @param templateURL URL that contains the ARM template to deploy. You must specify either `templateURL` OR `templateJSON`
#' @param paramURL URL that contains the template parameters. You must specify either `paramULR` OR `paramJSON`
#' @param templateJSON character vector that contains the ARM template to deploy. You must specify either `templateJSON` OR `templateURL`
#' @param paramJSON paramJSON
#'
#' @family Template functions
#' @export
azureDeployTemplate <- function(azureActiveContext, deplname, templateURL,
                                paramURL, templateJSON, paramJSON, mode = "Sync",
                                resourceGroup, subscriptionID,
                                verbose = FALSE) {
  assert_that(is.azureActiveContext(azureActiveContext))
  azureCheckToken(azureActiveContext)
  azToken <- azureActiveContext$Token
  if (missing(subscriptionID)) subscriptionID <- azureActiveContext$subscriptionID
  if (missing(resourceGroup)) resourceGroup <- azureActiveContext$resourceGroup

  assert_that(is_resource_group(resourceGroup))
  assert_that(is_subscription_id(subscriptionID))
  assert_that(is_deployment_name(deplname))

  if (missing(templateURL) && missing(templateJSON)) {
    stop("No templateURL or templateJSON provided")
  }

  verbosity <- set_verbosity(verbose)

  URL <- paste0("https://management.azure.com/subscriptions/", subscriptionID,
               "/resourceGroups/", resourceGroup, "/providers/microsoft.resources/deployments/",
               deplname, "?api-version=2016-06-01")

  combination <- paste0(if(!missing(templateURL)) "tu" else "tj" , 
                        if (!missing(paramURL)) "pu" else if (!missing(paramJSON)) "pj" else "")
  bodyI <- switch(
    combination,
    tu   = paste0('{"properties": ',
                  '{"templateLink": { "uri": "', templateURL, '","contentversion": "1.0.0.0"},',
                  '"mode": "Incremental","debugSetting": {"detailLevel": "requestContent, responseContent"}}}'),
    tupj = paste0('{"properties": {', paramJSON,
                  ',"templateLink": { "uri": "', templateURL, '","contentversion": "1.0.0.0"},',
                  '"mode": "Incremental","debugSetting": {"detailLevel": "requestContent, responseContent"}}}'),
    tupu = paste0('{"properties": {"templateLink": { "uri": "', templateURL, '","contentversion": "1.0.0.0"},',
                  '"mode": "Incremental",  "parametersLink": {"uri": "', paramURL, '","contentversion": "1.0.0.0"},',
                  '"debugSetting": {"detailLevel": "requestContent, responseContent"}}}'),
    tj   = paste0('{"properties": {"template": ', templateJSON,
                  ',"mode": "Incremental","debugSetting": {"detailLevel": "requestContent, responseContent"}}}'),
    tjpj = paste0('{"properties": {', paramJSON,
                  ',"template": ', templateJSON,
                  ',"mode": "Incremental","debugSetting": {"detailLevel": "requestContent, responseContent"}}}'),
    tjpu = paste0('{"properties": {"template": ', templateJSON,
                  ',  "mode": "Incremental",  "parametersLink": {"uri": "', paramURL,
                  '","contentversion": "1.0.0.0"},"debugSetting": {"detailLevel": "requestContent, responseContent"}}}')
  )

  r <- PUT(URL, azureApiHeaders(azToken), body = bodyI, verbosity)
  stopWithAzureError(r)
  
  if (toupper(mode) == "SYNC") {
    z <- pollStatusTemplate(azureActiveContext, deplname, resourceGroup)
    if(!z) return(FALSE)
  }
  message("")
  message(paste("Deployment", deplname, "submitted: ", Sys.time()))
  return(TRUE)
}


#' Check template deployment Status.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @inheritParams azureDeployTemplate
#'
#' @family Template functions
#' @export
azureDeployStatus <- function(azureActiveContext, deplname, resourceGroup,
                              subscriptionID, verbose = FALSE) {
  assert_that(is.azureActiveContext(azureActiveContext))
  azureCheckToken(azureActiveContext)
  azToken <- azureActiveContext$Token
  if (missing(subscriptionID)) subscriptionID <- azureActiveContext$subscriptionID
  if (missing(resourceGroup)) resourceGroup <- azureActiveContext$resourceGroup
  verbosity <- set_verbosity(verbose)
 

  assert_that(is_resource_group(resourceGroup))
  assert_that(is_subscription_id(subscriptionID))
  assert_that(is_deployment_name(deplname))

  URL <- paste0("https://management.azure.com/subscriptions/", subscriptionID,
               "/resourceGroups/", resourceGroup, "/providers/microsoft.resources/deployments/",
               deplname, "?api-version=2016-06-01")
  r <- GET(URL, azureApiHeaders(azToken), verbosity)
  stopWithAzureError(r)

  rl <- content(r, "text", encoding = "UTF-8")
  df <- fromJSON(rl)
  return(df)
}

azureDeployStatusSummary <- function(x) x$properties$provisioningState


#' Delete template deployment.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @inheritParams azureDeployTemplate
#'
#' @family Template functions
#' @export
azureDeleteDeploy <- function(azureActiveContext, deplname, resourceGroup,
                              subscriptionID, verbose = FALSE) {
  assert_that(is.azureActiveContext(azureActiveContext))
  azureCheckToken(azureActiveContext)
  azToken <- azureActiveContext$Token
  if (missing(subscriptionID)) subscriptionID <- azureActiveContext$subscriptionID
  if (missing(resourceGroup)) resourceGroup <- azureActiveContext$resourceGroup
  verbosity <- set_verbosity(verbose)
 
  assert_that(is_resource_group(resourceGroup))
  assert_that(is_subscription_id(subscriptionID))
  assert_that(is_deployment_name(deplname))

  URL <- paste("https://management.azure.com/subscriptions/", subscriptionID,
               "/resourceGroups/", resourceGroup, "/providers/microsoft.resources/deployments/",
               deplname, "?api-version=2016-06-01", sep = "")

  r <- DELETE(URL, azureApiHeaders(azToken), verbosity)
  rl <- content(r, "text", encoding = "UTF-8")
  stopWithAzureError(r)
  df <- fromJSON(rl)
  #print(df)
  return(TRUE)
}


#' Cancel template deployment.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @inheritParams azureDeployTemplate
#'
#' @family Template functions
#' @export
azureCancelDeploy <- function(azureActiveContext, deplname, resourceGroup,
                              subscriptionID, verbose = FALSE) {

  assert_that(is.azureActiveContext(azureActiveContext))
  azureCheckToken(azureActiveContext)
  azToken <- azureActiveContext$Token
  if (missing(subscriptionID)) subscriptionID <- azureActiveContext$subscriptionID
  if (missing(resourceGroup)) resourceGroup <- azureActiveContext$resourceGroup
  verbosity <- set_verbosity(verbose)
 
  assert_that(is_resource_group(resourceGroup))
  assert_that(is_subscription_id(subscriptionID))
  assert_that(is_deployment_name(deplname))

  URL <- paste0("https://management.azure.com/subscriptions/", subscriptionID,
               "/resourceGroups/", resourceGroup, "/providers/microsoft.resources/deployments/",
               deplname, "/cancel?api-version=2016-06-01")

  r <- POST(URL, azureApiHeaders(azToken), verbosity)
  stopWithAzureError(r)

  rl <- content(r, "text", encoding = "UTF-8")
  df <- fromJSON(rl)
  return(df$category)
}
