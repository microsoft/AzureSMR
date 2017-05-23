#' List scale sets within a resource group.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureListAllResources
#'
#' @family Virtual machine functions
#' @export
azureListScaleSets <- function(azureActiveContext, resourceGroup, location, subscriptionID,
                        verbose = FALSE) {
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

  URL <- paste("https://management.azure.com/subscriptions/", subscriptionID,
               "/resourceGroups/", resourceGroup, "/providers/Microsoft.Compute/virtualMachineScaleSets?api-version=2016-03-30",
               sep = "")

  r <- GET(URL, add_headers(.headers = c(Host = "management.azure.com",
                                         Authorization = azToken, `Content-type` = "application/json")), verbosity)
  rl <- content(r, "text", encoding = "UTF-8")
  df <- fromJSON(rl)
  dfn <- as.data.frame(df$value$name)
  clust <- nrow(dfn)
  if (clust < 1) {
    dfn[1:clust, 1] <- ""
    dfn[1:clust, 2] <- ""
    dfn[1:clust, 3] <- ""
    dfn[1:clust, 4] <- ""
    dfn[1:clust, 5] <- ""
    dfn[1:clust, 6] <- ""
    dfn[1:clust, 7] <- ""
    colnames(dfn) <- c("name", "location", "Skuname", "skutier", "capacity", "image",
                       "ver")
    print("No Virtual Machines scalesets found")
    return(dfn[-1,])
  }
  dfn[1:clust, 1] <- df$value$name
  dfn[1:clust, 2] <- df$value$location
  dfn[1:clust, 3] <- df$value$sku$name
  dfn[1:clust, 4] <- df$value$sku$tier
  dfn[1:clust, 5] <- df$value$sku$capacity
  dfn[1:clust, 6] <- df$value$properties$virtualMachineProfile$storageProfile$imageReference$offer
  dfn[1:clust, 7] <- df$value$properties$virtualMachineProfile$storageProfile$imageReference$sku

  colnames(dfn) <- c("name", "location", "Skuname", "skutier", "capacity", "image",
                     "ver")
  azureActiveContext$subscriptionID <- subscriptionID
  azureActiveContext$resourceGroup <- resourceGroup

  return(dfn)
}

#' List scale set network information with a ResourceGroup
#'
#' @inheritParams setAzureContext
#' @inheritParams azureListAllResources
#'
#' @family Virtual machine functions
#' @export
azureListScaleSetNetwork <- function(azureActiveContext, resourceGroup, location, subscriptionID,
                               verbose = FALSE) {
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

  URL <- paste("https://management.azure.com/subscriptions/", subscriptionID,
               "/resourceGroups/", resourceGroup, "/providers/Microsoft.Network/loadBalancers/", "?api-version=2016-03-30",
               sep = "")

  r <- GET(URL, add_headers(.headers = c(Host = "management.azure.com",
                                         Authorization = azToken, `Content-type` = "application/json")), verbosity)
  rl <- content(r, "text", encoding = "UTF-8")
  df <- fromJSON(rl)
  lbs <- df$value$name
  dfn <- data.frame(lbname = "", publicIpAdress = "", inport = "", outport = "")
  clust <- length(lbs)
  if (clust > 0) {
    for (lb in lbs) {
      URL <- paste("https://management.azure.com/subscriptions/", subscriptionID,
                    "/resourceGroups/", resourceGroup, "/providers/Microsoft.Network/loadBalancers/", lb, "?api-version=2016-03-30",
                    sep = "")
      r <- GET(URL, add_headers(.headers = c(Host = "management.azure.com",
                                              Authorization = azToken, `Content-type` = "application/json")), verbosity)
      rl <- content(r, "text", encoding = "UTF-8")
      df2 <- fromJSON(rl)
      dfn3 <- as.data.frame(df2$properties$inboundNatRules$name)
      clust2 <- nrow(dfn3)
      if (clust2 > 0) {
        dfn3[1:clust2, 2] <- ""
        dfn3[1:clust2, 3] <- ""
        dfn3[1:clust2, 4] <- df2$properties$inboundNatRules$properties$frontendPort
        dfn3[1:clust2, 5] <- df2$properties$inboundNatRules$properties$backendPort
      }
      if (nrow(dfn) == 1)
        dfn <- dfn3
      else
        dfn <- rbind(dfn, dfn3)
      }
    colnames(dfn) <- c("lbname", "domainName", "publicIP", "inPort", "outPort")
  }

  URL <- paste("https://management.azure.com/subscriptions/", subscriptionID,
               "/resourceGroups/", resourceGroup, "/providers/Microsoft.Network/publicIPAddresses", "?api-version=2016-03-30",
               sep = "")

  r <- GET(URL, add_headers(.headers = c(Host = "management.azure.com",
                                         Authorization = azToken, `Content-type` = "application/json")), verbosity)
  rl <- content(r, "text", encoding = "UTF-8")
  df <- fromJSON(rl)
  pips <- df$value$name
  clust <- length(lbs)

  if (clust > 0) {
    for (pip in pips) {
      URL <- paste("https://management.azure.com/subscriptions/", subscriptionID,
                   "/resourceGroups/", resourceGroup, "/providers/Microsoft.Network/publicIPAddresses/", pip, "?api-version=2016-03-30",
                   sep = "")
      r <- GET(URL, add_headers(.headers = c(Host = "management.azure.com",
                                             Authorization = azToken, `Content-type` = "application/json")), verbosity)
      rl <- content(r, "text", encoding = "UTF-8")
      df2 <- fromJSON(rl)
      clust <- nrow(dfn)
      clust2 <- length(df2$properties$ipAddress)
      if (clust2 > 0) {
        dfn[1:clust, 2] <- df2$properties$dnsSettings$fqdn
        dfn[1:clust, 3] <- df2$properties$ipAddress
      }
    }
  }

  azureActiveContext$subscriptionID <- subscriptionID
  azureActiveContext$resourceGroup <- resourceGroup

  return(dfn)
}

#' List VMs within a scale set
#'
#' @inheritParams setAzureContext
#' @inheritParams azureListAllResources
#' @param scaleSet name of the scale refer to (azureListScaleSets)
#'
#' @family Virtual machine functions
#' @export
azureListScaleSetVM <- function(azureActiveContext, scaleSet, resourceGroup, location, subscriptionID,
                                verbose = FALSE) {
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
  if (!length(scaleSet)) {
    stop("Error: No scaleSet provided")
  }
  if (!length(subscriptionID)) {
    stop("Error: No subscriptionID provided: Use SUBID argument or set in AzureContext")
  }
  if (!length(azToken)) {
    stop("Error: No Token / Not currently Authenticated")
  }


  URL <- paste("https://management.azure.com/subscriptions/", subscriptionID,
               "/resourceGroups/", resourceGroup, "/providers/Microsoft.Compute/virtualMachineScaleSets/", scaleSet, "/virtualMachines?api-version=2016-03-30",
               sep = "")

  r <- GET(URL, add_headers(.headers = c(Host = "management.azure.com",
                                         Authorization = azToken, `Content-type` = "application/json")), verbosity)
  rl <- content(r, "text", encoding = "UTF-8")
  df <- fromJSON(rl)
  dfn <- as.data.frame(df$value$name)
  clust <- nrow(dfn)
  if (clust < 1) {
    dfn[1:clust, 1] <- ""
    dfn[1:clust, 2] <- ""
    dfn[1:clust, 3] <- ""
    dfn[1:clust, 4] <- ""
    colnames(dfn) <- c("name", "id", "computerName", "state")
    print("No Virtual Machines scalesets found")
    return(dfn[-1,])
  }
  dfn[1:clust, 1] <- df$value$name
  dfn[1:clust, 2] <- df$value$instanceId
  dfn[1:clust, 3] <- df$value$properties$osProfile$computerName
  dfn[1:clust, 4] <- df$value$properties$provisioningState

  colnames(dfn) <- c("name", "id", "computerName", "state")
  azureActiveContext$subscriptionID <- subscriptionID
  azureActiveContext$resourceGroup <- resourceGroup

  return(dfn)
}
