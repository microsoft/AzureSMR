#'List scale sets within a resource group.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureListAllResources
#'
#' @family Virtual machine functions
#' @export
azureListScaleSets <- function(azureActiveContext, resourceGroup, 
                               location, subscriptionID,
                               verbose = FALSE) {
  
  assert_that(is.azureActiveContext(azureActiveContext))
  if(missing(subscriptionID)) subscriptionID <- azureActiveContext$subscriptionID
  if(missing(resourceGroup)) resourceGroup <- azureActiveContext$resourceGroup
  if(!is.null(resourceGroup)) assert_that(is_resource_group(resourceGroup))
  assert_that(is_subscription_id(subscriptionID))

  rg <- if(!is.null(resourceGroup)) paste0("/resourceGroups/", resourceGroup) else ""

  uri <- paste0(
    "https://management.azure.com/subscriptions/", subscriptionID,
    rg,
    "/providers/Microsoft.Compute/virtualMachineScaleSets?api-version=2016-03-30"
  )

  r <- call_azure_sm(azureActiveContext, uri = uri,
    verb = "GET", verbose = verbose)
  stopWithAzureError(r)

  rl <- content(r, "text", encoding = "UTF-8")
  dat <- fromJSON(rl)$value
  if (length(dat$name) == 0) {
    message("No Virtual Machines scalesets found")
  }
  dfn <- data.frame(
      name     = dat$name,
      location = dat$location,
      skuname  = dat$sku$name,
      skutier  = dat$sku$tier,
      capacity = dat$sku$capacity,
      image    = dat$properties$virtualMachineProfile$storageProfile$imageReference$offer,
      ver = dat$properties$virtualMachineProfile$storageProfile$imageReference$sku,
      subscriptionID = extractSubscriptionID(dat$id),
      resourceGroup = extractResourceGroupname(dat$id),
      stringsAsFactors = FALSE
    )
  azureActiveContext$subscriptionID <- subscriptionID
  azureActiveContext$resourceGroup <- resourceGroup

  return(dfn)
}


#' List load balancers and ip addresses in a resource group
#'
#' @inheritParams setAzureContext
#' @inheritParams azureListAllResources
#'
#' @family Virtual machine functions
#' @references https://docs.microsoft.com/en-us/rest/api/network/loadbalancer/list-load-balancers-within-a-resource-group
#' @export
azureListScaleSetNetwork <- function(azureActiveContext, resourceGroup, 
                                     location, subscriptionID,
                                     verbose = FALSE) {
  
  assert_that(is.azureActiveContext(azureActiveContext))
  if (missing(subscriptionID)) subscriptionID <- azureActiveContext$subscriptionID
  if (missing(resourceGroup)) resourceGroup <- azureActiveContext$resourceGroup
  if (missing(resourceGroup)) assert_that(is_resource_group(resourceGroup))
  assert_that(is_subscription_id(subscriptionID))

  rg <- if (!is.null(resourceGroup)) paste0("/resourceGroups/", resourceGroup) else "/"

  uri <- paste0(
    "https://management.azure.com/subscriptions/", subscriptionID,
    rg,
    "/providers/Microsoft.Network/loadBalancers", 
    "?api-version=2016-09-01")

  r <- call_azure_sm(azureActiveContext, uri = uri, 
    verb = "GET", verbose = verbose)
  stopWithAzureError(r)

  rl <- content(r, "text", encoding = "UTF-8")
  df <- fromJSON(rl)
  lbs <- df$value$name
  dfn <- data.frame(lbname = "", publicIpAdress = "", inport = "", outport = "")
  clust <- length(lbs)
  if (clust > 0) {
    for (i in seq_along(lbs)) {
      lb <- lbs[i]
      resgroup <- extractResourceGroupname(df$value$id[i])
      uri <- paste0("https://management.azure.com/subscriptions/", subscriptionID,
                    "/resourceGroups/", resgroup,
                    "/providers/Microsoft.Network/loadBalancers/", lb, 
                    "?api-version=2016-09-01")
      
      r <- call_azure_sm(azureActiveContext, uri = uri,
                         verb = "GET", verbose = verbose)
      stopWithAzureError(r)
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


  uri <- paste0("https://management.azure.com/subscriptions/", subscriptionID,
               rg, 
               "/providers/Microsoft.Network/publicIPAddresses", 
               "?api-version=2016-09-01")
  r <- call_azure_sm(azureActiveContext, uri = uri,
    verb = "GET", verbose = verbose)
  stopWithAzureError(r)

  rl <- content(r, "text", encoding = "UTF-8")
  df <- fromJSON(rl)
  pips <- df$value$name
  clust <- length(lbs)

  if (clust > 0) {
    pips <- lapply(seq_along(pips), function(i) {
      pip <- pips[i]
      resgroup <- extractResourceGroupname(df$value$id[i])
      uri <- paste0(
        "https://management.azure.com/subscriptions/", subscriptionID,
        "/resourceGroups/", resgroup, 
        "/providers/Microsoft.Network/publicIPAddresses/", pip, 
        "?api-version=2016-09-01"
      )
      r <- call_azure_sm(azureActiveContext, uri = uri,
                         verb = "GET", verbose = verbose)
      stopWithAzureError(r)

      rl <- content(r, "text", encoding = "UTF-8")
      df2 <- fromJSON(rl)
      clust <- nrow(dfn)
      clust2 <- length(df2$properties$ipAddress)
      if (clust2 > 0) {
        data.frame(
          fqdn <-  if (is.null(df2$properties$dnsSettings$fqdn)) "" else 
            df2$properties$dnsSettings$fqdn,
          ipAddress = df2$properties$ipAddress,
          stringsAsFactors = FALSE
        )
      } else {
        data.frame(
          fqdn = character(0),
          ipAddress = character(0)
        )
      }
    })
    pips <- do.call(rbind, pips)
  }

  azureActiveContext$subscriptionID <- subscriptionID
  azureActiveContext$resourceGroup <- resourceGroup

  return(list(dfn, pips))
}


#' List VMs within a scale set
#'
#' @inheritParams setAzureContext
#' @inheritParams azureListAllResources
#' @param scaleSet Character vector with name of the scaleset (see also [azureListScaleSets()])
#'
#' @family Virtual machine functions
#' @export
azureListScaleSetVM <- function(azureActiveContext, scaleSet, resourceGroup, location, subscriptionID,
                                verbose = FALSE) {
  assert_that(is.azureActiveContext(azureActiveContext))
  if (missing(subscriptionID)) subscriptionID <- azureActiveContext$subscriptionID
  if (missing(resourceGroup)) resourceGroup <- azureActiveContext$resourceGroup
  assert_that(is_resource_group(resourceGroup))
  assert_that(is_subscription_id(subscriptionID))
  assert_that(is_scaleset(scaleSet))

  uri <- paste0("https://management.azure.com/subscriptions/", subscriptionID,
               "/resourceGroups/", resourceGroup, 
               "/providers/Microsoft.Compute/virtualMachineScaleSets/", scaleSet, 
               "/virtualMachines?api-version=2016-03-30")

  r <- call_azure_sm(azureActiveContext, uri = uri, 
    verb = "GET", verbose = verbose)
  stopWithAzureError(r)

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
