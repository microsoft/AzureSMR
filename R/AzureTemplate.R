#' Deploy an Azure Resource Manager Template.
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
                                azToken, verbose = FALSE) {
  azureCheckToken(azureActiveContext)

  if (missing(azToken)) {
    AT <- azureActiveContext$Token
  } else (AT <- azToken)
  if (missing(subscriptionID)) {
    SUBIDI <- azureActiveContext$subscriptionID
  } else (SUBIDI <- subscriptionID)
  if (missing(resourceGroup)) {
    RGI <- azureActiveContext$resourceGroup
  } else (RGI <- resourceGroup)

  if (!length(RGI)) {
    stop("Error: No resourceGroup provided: Use resourceGroup argument or set in AzureContext")
  }
  if (!length(SUBIDI)) {
    stop("Error: No subscriptionID provided: Use SUBID argument or set in AzureContext")
  }
  if (!length(AT)) {
    stop("Error: No Token / Not currently Authenticated")
  }
  if (!length(deplname)) {
    stop("No deplname provided")
  }

  if (missing(templateURL) && missing(templateJSON)) {
    stop("No templateURL or templateJSON provided")
  }

  verbosity <- if (verbose)
    httr::verbose(TRUE) else NULL

  URL <- paste("https://management.azure.com/subscriptions/", SUBIDI,
               "/resourceGroups/", RGI, "/providers/microsoft.resources/deployments/",
               deplname, "?api-version=2016-06-01", sep = "")
  # print(URL)

  if (missing(templateJSON)) {
    if (missing(paramURL)) {
      if (missing(paramJSON))
        bodyI <- paste("{\"properties\": {\"templateLink\": { \"uri\": \"",
                       templateURL, "\",\"contentversion\": \"1.0.0.0\"},\"mode\": \"Incremental\",\"debugSetting\": {\"detailLevel\": \"requestContent, responseContent\"}}}",
                       sep = "") else bodyI <- paste("{\"properties\": {", paramJSON, ",\"templateLink\": { \"uri\": \"",
                                                     templateURL, "\",\"contentversion\": \"1.0.0.0\"},\"mode\": \"Incremental\",\"debugSetting\": {\"detailLevel\": \"requestContent, responseContent\"}}}",
                                                     sep = "")
    } else bodyI <- paste("{\"properties\": {\"templateLink\": { \"uri\": \"",
                          templateURL, "\",\"contentversion\": \"1.0.0.0\"},  \"mode\": \"Incremental\",  \"parametersLink\": {\"uri\": \"",
                          paramURL, "\",\"contentversion\": \"1.0.0.0\"},\"debugSetting\": {\"detailLevel\": \"requestContent, responseContent\"}}}",
                          sep = "")
  } else {
    if (missing(paramURL)) {
      if (missing(paramJSON))
        bodyI <- paste("{\"properties\": {\"template\": ", templateJSON,
                       ",\"mode\": \"Incremental\",\"debugSetting\": {\"detailLevel\": \"requestContent, responseContent\"}}}",
                       sep = "") else bodyI <- paste("{\"properties\": {", paramJSON, ",\"template\": ",
                                                     templateJSON, ",\"mode\": \"Incremental\",\"debugSetting\": {\"detailLevel\": \"requestContent, responseContent\"}}}",
                                                     sep = "")
    } else bodyI <- paste("{\"properties\": {\"template\": ", templateJSON,
                          ",  \"mode\": \"Incremental\",  \"parametersLink\": {\"uri\": \"",
                          paramURL, "\",\"contentversion\": \"1.0.0.0\"},\"debugSetting\": {\"detailLevel\": \"requestContent, responseContent\"}}}",
                          sep = "")
  }

  r <- PUT(URL, add_headers(.headers = c(Host = "management.azure.com",
                                         Authorization = AT, `Content-type` = "application/json")), body = bodyI,
           verbosity)
  # print(paste(deplname,'Submitted'))
  if (status_code(r) != 200 && status_code(r) != 201 && status_code(r) !=
      202) {
    stopWithAzureError(r)
  }
  rl <- content(r, "text", encoding = "UTF-8")
  # print (rl)
  df <- fromJSON(rl)
  if (toupper(mode) == "SYNC") {
    rc <- "running"
    writeLines(paste("azureDeployTemplate: Request Submitted: ", Sys.time()))
    writeLines("Running(R), Succeeded(S)")
    a <- 1
    while (a > 0) {
      rc <- azureDeployStatus(azureActiveContext, deplname = deplname,
                              resourceGroup = RGI)
      if (grepl("Succeeded", rc)) {
        writeLines("")
        writeLines(paste("Finished Deploying Sucessfully: ", Sys.time()))
        (break)()
      }
      if (grepl("Error", rc) || grepl("Failed", rc)) {
        writeLines("")
        writeLines(paste("Error Deploying: ", Sys.time()))
        (break)()
      }

      a <- a + 1
      if (grepl("Succeeded", rc)) {
        rc <- "S"
      } else if (grepl("Running", rc)) {
        rc <- "R"
      } else if (grepl("updating", rc)) {
        rc <- "U"
      } else if (grepl("Starting", rc)) {
        rc <- "S"
      } else if (grepl("Accepted", rc)) {
        rc <- "A"
      }

      cat(rc)

      if (a > 500)
        (break)()
      Sys.sleep(5)
    }
  }
  writeLines(paste("Deployment", deplname, "Submitted: ", Sys.time()))
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
                              subscriptionID, azToken, verbose = FALSE) {
  azureCheckToken(azureActiveContext)
  if (missing(azToken)) {
    AT <- azureActiveContext$Token
  } else (AT <- azToken)
  if (missing(subscriptionID)) {
    SUBIDI <- azureActiveContext$subscriptionID
  } else (SUBIDI <- subscriptionID)
  if (missing(resourceGroup)) {
    RGI <- azureActiveContext$resourceGroup
  } else (RGI <- resourceGroup)
  verbosity <- if (verbose)
    httr::verbose(TRUE) else NULL

  if (!length(RGI)) {
    stop("Error: No resourceGroup provided: Use resourceGroup argument or set in AzureContext")
  }
  if (!length(SUBIDI)) {
    stop("Error: No subscriptionID provided: Use SUBID argument or set in AzureContext")
  }
  if (!length(AT)) {
    stop("Error: No Token / Not currently Authenticated")
  }
  if (!length(deplname)) {
    stop("No deplname provided")
  }

  URL <- paste("https://management.azure.com/subscriptions/", SUBIDI,
               "/resourceGroups/", RGI, "/providers/microsoft.resources/deployments/",
               deplname, "?api-version=2016-06-01", sep = "")
  # print(URL)

  r <- GET(URL, add_headers(.headers = c(Host = "management.azure.com",
                                         Authorization = AT, `Content-type` = "application/json")), verbosity)
  rl <- content(r, "text", encoding = "UTF-8")

  df <- fromJSON(rl)
  # print(df)
  return(df$properties$provisioningState)
}


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
                              subscriptionID, azToken, verbose = FALSE) {
  azureCheckToken(azureActiveContext)

  if (missing(azToken)) {
    AT <- azureActiveContext$Token
  } else (AT <- azToken)
  if (missing(subscriptionID)) {
    SUBIDI <- azureActiveContext$subscriptionID
  } else (SUBIDI <- subscriptionID)
  if (missing(resourceGroup)) {
    RGI <- azureActiveContext$resourceGroup
  } else (RGI <- resourceGroup)
  verbosity <- if (verbose)
    httr::verbose(TRUE) else NULL

  if (!length(RGI)) {
    stop("Error: No resourceGroup provided: Use resourceGroup argument or set in AzureContext")
  }
  if (!length(SUBIDI)) {
    stop("Error: No subscriptionID provided: Use SUBID argument or set in AzureContext")
  }
  if (!length(AT)) {
    stop("Error: No Token / Not currently Authenticated")
  }
  if (!length(deplname)) {
    stop("No deplname provided")
  }

  URL <- paste("https://management.azure.com/subscriptions/", SUBIDI,
               "/resourceGroups/", RGI, "/providers/microsoft.resources/deployments/",
               deplname, "?api-version=2016-06-01", sep = "")
  # print(URL)

  r <- DELETE(URL, add_headers(.headers = c(Host = "management.azure.com",
                                            Authorization = AT, `Content-type` = "application/json")))
  print(http_status(r))
  rl <- content(r, "text", encoding = "UTF-8")
  print(rl)
  df <- fromJSON(rl)
  print(df)
  return("OK")
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
                              subscriptionID, azToken, verbose = FALSE) {

  azureCheckToken(azureActiveContext)

  if (missing(azToken)) {
    AT <- azureActiveContext$Token
  } else (AT <- azToken)
  if (missing(subscriptionID)) {
    SUBIDI <- azureActiveContext$subscriptionID
  } else (SUBIDI <- subscriptionID)
  if (missing(resourceGroup)) {
    RGI <- azureActiveContext$resourceGroup
  } else (RGI <- resourceGroup)
  verbosity <- if (verbose)
    httr::verbose(TRUE) else NULL

  if (!length(RGI)) {
    stop("Error: No resourceGroup provided: Use resourceGroup argument or set in AzureContext")
  }
  if (!length(SUBIDI)) {
    stop("Error: No subscriptionID provided: Use SUBID argument or set in AzureContext")
  }
  if (!length(AT)) {
    stop("Error: No Token / Not currently Authenticated")
  }
  if (!length(deplname)) {
    stop("No deplname provided")
  }

  URL <- paste("https://management.azure.com/subscriptions/", SUBIDI,
               "/resourceGroups/", RGI, "/providers/microsoft.resources/deployments/",
               deplname, "/cancel?api-version=2016-06-01", sep = "")
  # print(URL)

  r <- POST(URL, add_headers(.headers = c(Host = "management.azure.com",
                                          Authorization = AT, `Content-type` = "application/json")), verbosity)
  rl <- content(r, "text", encoding = "UTF-8")
  df <- fromJSON(rl)
  return(df$category)
}
