#' Deploy a resource from an Azure Resource Manager (ARM) template.
#'
#' Deploy a resource from a template. See https://github.com/Azure/azure-quickstart-templates
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @inheritParams azureResizeHDI
#'
#' @param deplname deplname
#' @param templateURL templateURL
#' @param paramURL paramURL
#' @param templateJSON templateJSON
#' @param paramJSON paramJSON
#'
#' @family Template functions
#' @export
azureDeployTemplate <- function(azureActiveContext, deplname, templateURL,
                                paramURL, templateJSON, paramJSON, mode = "Sync",
                                resourceGroup, subscriptionID,
                                verbose = FALSE) {
  azureCheckToken(azureActiveContext)

  azToken <- azureActiveContext$Token
  if (missing(subscriptionID)) {
    subscriptionID <- azureActiveContext$subscriptionID
  } 
  if (missing(resourceGroup)) {
    resourceGroup <- azureActiveContext$resourceGroup
  } 

  if (!length(resourceGroup)) {
    stop("Error: No resourceGroup provided: Use resourceGroup argument or set in AzureContext")
  }
  if (!length(subscriptionID)) {
    stop("Error: No subscriptionID provided: Use SUBID argument or set in AzureContext")
  }
  if (!length(azToken)) {
    stop("Error: No Token / Not currently Authenticated")
  }
  if (!length(deplname)) {
    stop("No deplname provided")
  }

  if (missing(templateURL) && missing(templateJSON)) {
    stop("No templateURL or templateJSON provided")
  }

  verbosity <- set_verbosity(verbose)
 

  URL <- paste("https://management.azure.com/subscriptions/", subscriptionID,
               "/resourceGroups/", resourceGroup, "/providers/microsoft.resources/deployments/",
               deplname, "?api-version=2016-06-01", sep = "")

  bodyI <- if (missing(templateJSON)) {
    if (missing(paramURL)) {
      if (missing(paramJSON)) {
        paste0('{"properties": ',
             '{"templateLink": { "uri": "', templateURL, '","contentversion": "1.0.0.0"},',
             '"mode": "Incremental","debugSetting": {"detailLevel": "requestContent, responseContent"}}}'
      )
      } else {
        paste0('{"properties": {', paramJSON,
             ',"templateLink": { "uri": "', templateURL, '","contentversion": "1.0.0.0"},',
             '"mode": "Incremental","debugSetting": {"detailLevel": "requestContent, responseContent"}}}'
      )
      }
    } else {
      paste0('{"properties": {"templateLink": { "uri": "', templateURL, '","contentversion": "1.0.0.0"},',
           '"mode": "Incremental",  "parametersLink": {"uri": "', paramURL, '","contentversion": "1.0.0.0"},',
           '"debugSetting": {"detailLevel": "requestContent, responseContent"}}}'
    )
    }
  } else {
    if (missing(paramURL)) {
      if (missing(paramJSON)) {
        paste0('{"properties": {"template": ', templateJSON,
             ',"mode": "Incremental","debugSetting": {"detailLevel": "requestContent, responseContent"}}}'
      )
      } else {
        paste0('{"properties": {', paramJSON,
             ',"template": ', templateJSON,
             ',"mode": "Incremental","debugSetting": {"detailLevel": "requestContent, responseContent"}}}'
      )
      }
    } else {
      paste0('{"properties": {"template": ', templateJSON,
           ',  "mode": "Incremental",  "parametersLink": {"uri": "', paramURL,
           '","contentversion": "1.0.0.0"},"debugSetting": {"detailLevel": "requestContent, responseContent"}}}')
    }
  }

  r <- PUT(URL, add_headers(.headers = c(Host = "management.azure.com",
                                         Authorization = azToken, `Content-type` = "application/json")), body = bodyI,
           verbosity)

  stopWithAzureError(r)
  
  if (toupper(mode) == "SYNC") {
    z <- pollStatusTemplate(azureActiveContext, deplname, resourceGroup)
    if(!z) return(FALSE)
  }
  message("")
  message(paste("Deployment", deplname, "submitted: ", Sys.time()))
  return(TRUE)
}




#' Check Template Deployment Status.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @inheritParams azureDeployTemplate
#'
#' @family Template functions
#' @export
azureDeployStatus <- function(azureActiveContext, deplname, resourceGroup,
                              subscriptionID, verbose = FALSE) {
  azureCheckToken(azureActiveContext)
  azToken <- azureActiveContext$Token
  if (missing(subscriptionID)) {
    subscriptionID <- azureActiveContext$subscriptionID
  } else (subscriptionID <- subscriptionID)
  if (missing(resourceGroup)) {
    resourceGroup <- azureActiveContext$resourceGroup
  } else (resourceGroup <- resourceGroup)
  verbosity <- set_verbosity(verbose)
 

  if (!length(resourceGroup)) {
    stop("Error: No resourceGroup provided: Use resourceGroup argument or set in AzureContext")
  }
  if (!length(subscriptionID)) {
    stop("Error: No subscriptionID provided: Use SUBID argument or set in AzureContext")
  }
  if (!length(azToken)) {
    stop("Error: No Token / Not currently Authenticated")
  }
  if (!length(deplname)) {
    stop("No deplname provided")
  }

  URL <- paste("https://management.azure.com/subscriptions/", subscriptionID,
               "/resourceGroups/", resourceGroup, "/providers/microsoft.resources/deployments/",
               deplname, "?api-version=2016-06-01", sep = "")
  # print(URL)

  r <- GET(URL, add_headers(.headers = c(Host = "management.azure.com",
                                         Authorization = azToken, `Content-type` = "application/json")), verbosity)
  rl <- content(r, "text", encoding = "UTF-8")

  df <- fromJSON(rl)
  # print(df)
  return(df)
}

azureDeployStatusSummary <- function(x) x$properties$provisioningState


#' Delete Template Deployment.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @inheritParams azureDeployTemplate
# @param azureActiveContext Azure Context Object @param deplname
# deplname
#'
#' @family Template functions
#' @export
azureDeleteDeploy <- function(azureActiveContext, deplname, resourceGroup,
                              subscriptionID, verbose = FALSE) {
  azureCheckToken(azureActiveContext)

  azToken <- azureActiveContext$Token
  if (missing(subscriptionID)) {
    subscriptionID <- azureActiveContext$subscriptionID
  } else (subscriptionID <- subscriptionID)
  if (missing(resourceGroup)) {
    resourceGroup <- azureActiveContext$resourceGroup
  } else (resourceGroup <- resourceGroup)
  verbosity <- set_verbosity(verbose)
 

  if (!length(resourceGroup)) {
    stop("Error: No resourceGroup provided: Use resourceGroup argument or set in AzureContext")
  }
  if (!length(subscriptionID)) {
    stop("Error: No subscriptionID provided: Use SUBID argument or set in AzureContext")
  }
  if (!length(azToken)) {
    stop("Error: No Token / Not currently Authenticated")
  }
  if (!length(deplname)) {
    stop("No deplname provided")
  }

  URL <- paste("https://management.azure.com/subscriptions/", subscriptionID,
               "/resourceGroups/", resourceGroup, "/providers/microsoft.resources/deployments/",
               deplname, "?api-version=2016-06-01", sep = "")
  # print(URL)

  r <- DELETE(URL, add_headers(.headers = c(Host = "management.azure.com",
                                            Authorization = azToken, `Content-type` = "application/json")))
  print(http_status(r))
  rl <- content(r, "text", encoding = "UTF-8")
  print(rl)
  df <- fromJSON(rl)
  print(df)
  return(TRUE)
}

#' Cancel Template Deployment.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @inheritParams azureDeployTemplate
#'
#' @family Template functions
#' @export
azureCancelDeploy <- function(azureActiveContext, deplname, resourceGroup,
                              subscriptionID, verbose = FALSE) {

  azureCheckToken(azureActiveContext)

  azToken <- azureActiveContext$Token
  if (missing(subscriptionID)) {
    subscriptionID <- azureActiveContext$subscriptionID
  } else (subscriptionID <- subscriptionID)
  if (missing(resourceGroup)) {
    resourceGroup <- azureActiveContext$resourceGroup
  } else (resourceGroup <- resourceGroup)
  verbosity <- set_verbosity(verbose)
 

  if (!length(resourceGroup)) {
    stop("Error: No resourceGroup provided: Use resourceGroup argument or set in AzureContext")
  }
  if (!length(subscriptionID)) {
    stop("Error: No subscriptionID provided: Use SUBID argument or set in AzureContext")
  }
  if (!length(azToken)) {
    stop("Error: No Token / Not currently Authenticated")
  }
  if (!length(deplname)) {
    stop("No deplname provided")
  }

  URL <- paste("https://management.azure.com/subscriptions/", subscriptionID,
               "/resourceGroups/", resourceGroup, "/providers/microsoft.resources/deployments/",
               deplname, "/cancel?api-version=2016-06-01", sep = "")
  # print(URL)

  r <- POST(URL, add_headers(.headers = c(Host = "management.azure.com",
                                          Authorization = azToken, `Content-type` = "application/json")), verbosity)
  rl <- content(r, "text", encoding = "UTF-8")
  df <- fromJSON(rl)
  return(df$category)
}
