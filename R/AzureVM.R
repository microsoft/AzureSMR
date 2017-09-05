#' List VMs in a Subscription.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureListAllResources
#'
#' @family Virtual machine functions
#' @export
azureListVM <- function(azureActiveContext, resourceGroup, location, subscriptionID,
                        verbose = FALSE) {
  assert_that(is.azureActiveContext(azureActiveContext))
  if (missing(subscriptionID)) subscriptionID <- azureActiveContext$subscriptionID 
  if (missing(resourceGroup)) resourceGroup <- azureActiveContext$resourceGroup 
  assert_that(is_resource_group(resourceGroup))
  assert_that(is_subscription_id(subscriptionID))
  
  uri <- paste0("https://management.azure.com/subscriptions/", subscriptionID,
               "/resourceGroups/", resourceGroup, 
               "/providers/Microsoft.Compute/virtualmachines?api-version=2015-05-01-preview"
               )

  r <- call_azure_sm(azureActiveContext, uri = uri,
    verb = "GET", verbose = verbose)
  stopWithAzureError(r)

  rl <- content(r, "text", encoding = "UTF-8")
  df <- fromJSON(rl)
  dfn <- as.data.frame(df$value$name, stringsAsFactors = FALSE)
  clust <- nrow(dfn)
  if (clust < 1) {
    warning("No Virtual Machines found")
    return(NULL)
  }
  dfn[1:clust, 1] <- df$value$name
  dfn[1:clust, 2] <- df$value$location
  dfn[1:clust, 3] <- df$value$type
  dfn[1:clust, 4] <-  if (!is.null(df$value$properties$storageProfile$osDisk$osType))
     df$value$properties$storageProfile$osDisk$osType
  else
    "-"
  dfn[1:clust, 5] <- df$value$properties$provisioningState
  dfn[1:clust, 6] <- df$value$properties$osProfile$adminUsername
  dfn[1:clust, 7] <- df$value$id
  colnames(dfn) <- c("name", "location", "type", "OS", "state", "admin", "ID")

  azureActiveContext$subscriptionID <- subscriptionID
  azureActiveContext$resourceGroup <- resourceGroup

  return(dfn)
}

#' List the status of every virtual machine in the azure active context.
#'
#' First queries the azure active context for all visible resources, then sequentially queries the status of all virtuam machines.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureListAllResources
#'
#' @family Virtual machine functions
#' @export
azureGetAllVMstatus <- function(azureActiveContext) {
  res <- azureListAllResources(azureActiveContext)
  vms <- res[res$type == "Microsoft.Compute/virtualMachines",]
  n <- nrow(vms)
  pb <- txtProgressBar(min = 0, max = n, style = 3)
  on.exit(close(pb))
  z <- lapply(seq_len(n), function(i) {
    setTxtProgressBar(pb, i)
    z <- tryCatch(azureVMStatus(azureActiveContext,
                  resourceGroup = vms$resourceGroup[i],
                  vmName = vms$name[i],
                  subscriptionID = vms$subscriptionID[i]
                  ), error = function(e) e)
    z <- if (inherits(z, "error")) NA_character_ else z
    data.frame(
      name = vms$name[i],
      resourceGroup = vms$resourceGroup[i],
      subscriptionID = vms$subscriptionID[i],
      provisioning = gsub("Provisioning (.*?), .*$", "\\1", z),
      status = gsub(".*?, VM (.*)$", "\\1", z)
    )
  })
  do.call(rbind, z)
}



#' Start a Virtual Machine.
#'
#' @inheritParams azureListVM
#' @param vmName Virtual Machine name
#' @param mode Wait for operation to complete 'Sync' (Default)
#'
#' @family Virtual machine functions
#' @export
azureStartVM <- function(azureActiveContext, resourceGroup, vmName, mode = "Sync",
                         subscriptionID, verbose = FALSE) {
  assert_that(is.azureActiveContext(azureActiveContext))

  if(missing(resourceGroup)) resourceGroup <- azureActiveContext$resourceGroup
  if(missing(subscriptionID)) subscriptionID <- azureActiveContext$subscriptionID
  if(missing(vmName)) vmName <- azureActiveContext$vmName

  assert_that(is_resource_group(resourceGroup))
  assert_that(is_subscription_id(subscriptionID))
  assert_that(is_vm_name(vmName))
  
  uri <- paste0("https://management.azure.com/subscriptions/", subscriptionID,
              "/resourceGroups/", resourceGroup, "/providers/Microsoft.Compute/virtualmachines/",
              vmName, "/start?api-version=2015-05-01-preview")

  r <- call_azure_sm(azureActiveContext, uri = uri,
    verb = "POST", verbose = verbose)
  stopWithAzureError(r)

  rl <- content(r, "text", encoding = "UTF-8")

  azureActiveContext$subscriptionID <- subscriptionID
  azureActiveContext$resourceGroup <- resourceGroup
  azureActiveContext$vmName <- vmName

  if (toupper(mode) == "SYNC") {
    z <- pollStatusVM(azureActiveContext)
    if (!z) return(FALSE)
    }

  message(paste("Start request submitted: ", Sys.time()))
  return(TRUE)
}


#' Stop a Virtual Machine.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureListVM
#' @inheritParams azureStartVM
#'
#' @family Virtual machine functions
#' @export
azureStopVM <- function(azureActiveContext, resourceGroup, vmName, mode = "Sync",
                        subscriptionID, verbose = FALSE) {
  assert_that(is.azureActiveContext(azureActiveContext))

  if (missing(subscriptionID)) subscriptionID <- azureActiveContext$subscriptionID
  if (missing(resourceGroup)) resourceGroup <- azureActiveContext$resourceGroup
  if (missing(vmName)) vmName <- azureActiveContext$vmName

  assert_that(is_resource_group(resourceGroup))
  assert_that(is_subscription_id(subscriptionID))
  assert_that(is_vm_name(vmName))
     
  uri <- paste0("https://management.azure.com/subscriptions/", subscriptionID,
               "/resourceGroups/", resourceGroup, 
               "/providers/Microsoft.Compute/virtualmachines/", vmName, 
               "/deallocate?api-version=2015-05-01-preview")
  
  r <- call_azure_sm(azureActiveContext, uri = uri, 
    verb = "POST", verbose = verbose)
  stopWithAzureError(r)

  rl <- content(r, "text", encoding = "UTF-8")

  azureActiveContext$subscriptionID <- subscriptionID
  azureActiveContext$resourceGroup <- resourceGroup
  azureActiveContext$vmName <- vmName

  if (toupper(mode) == "SYNC") {
    z <- pollStatusVM(azureActiveContext)
    if (!z) return(FALSE)
  }

  message("Deallocated: ", Sys.time())
  return(TRUE)
}


#' Get Status of a Virtual Machine.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureListVM
#' @inheritParams azureStartVM
#' @param ignore ignore
#'
#' @family Virtual machine functions
#' @export
azureVMStatus <- function(azureActiveContext, resourceGroup, vmName, subscriptionID,
                          ignore = "N", verbose = FALSE) {
  assert_that(is.azureActiveContext(azureActiveContext))

  if (missing(subscriptionID)) subscriptionID <- azureActiveContext$subscriptionID
  if (missing(resourceGroup)) resourceGroup <- azureActiveContext$resourceGroup
  if (missing(vmName)) vmName <- azureActiveContext$vmName
     
  assert_that(is_resource_group(resourceGroup))
  assert_that(is_subscription_id(subscriptionID))
  assert_that(is_vm_name(vmName))

  uri <- paste0("https://management.azure.com/subscriptions/", subscriptionID,
               "/resourceGroups/", resourceGroup, 
               "/providers/Microsoft.Compute/virtualmachines/", vmName, 
               "/InstanceView?api-version=2015-05-01-preview")
  
  r <- call_azure_sm(azureActiveContext, uri = uri,
    verb = "GET", verbose = verbose)
  if(status_code(r) == 404 && ignore == "Y") return("NA")
  stopWithAzureError(r)

  rl <- content(r, "text", encoding = "UTF-8")
  df <- fromJSON(rl)

  dfn <- as.data.frame(df$statuses)
  
  clust <- nrow(dfn)
  if (clust < 1) {
    if (ignore == "Y") {
      return("NA")
    } else {
      stop("No Virtual Machines found")
    }
  }
  
  return(paste(df$statuses$displayStatus, collapse=", "))
}

#' Delete a Virtual Machine.
#'
#' @inheritParams azureListVM
#' @inheritParams azureStartVM
#' @family Virtual machine functions
#' @export
azureDeleteVM <- function(azureActiveContext, resourceGroup, vmName, subscriptionID,
                          mode = "Sync", verbose = FALSE) {
  assert_that(is.azureActiveContext(azureActiveContext))

  if (missing(subscriptionID)) subscriptionID <- azureActiveContext$subscriptionID
  if (missing(resourceGroup)) resourceGroup <- azureActiveContext$resourceGroup
  if (missing(vmName)) vmName <- azureActiveContext$vmName
     
  assert_that(is_resource_group(resourceGroup))
  assert_that(is_subscription_id(subscriptionID))
  assert_that(is_vm_name(vmName))

  uri <- paste0("https://management.azure.com/subscriptions/", subscriptionID,
               "/resourceGroups/", resourceGroup, 
               "/providers/Microsoft.Compute/virtualmachines/", vmName, 
               "?api-version=2015-05-01-preview")

  r <- call_azure_sm(azureActiveContext, uri = uri, 
    verb = "DELETE", verbose = verbose)
  stopWithAzureError(r)

  azureActiveContext$subscriptionID <- subscriptionID
  azureActiveContext$resourceGroup <- resourceGroup
  azureActiveContext$vmName <- vmName

  if (toupper(mode) == "SYNC") {
    z <- pollStatusVM(azureActiveContext)
    if (!z) return(FALSE)
  }
  message(paste("Finished: ", Sys.time()))
  return(TRUE)
}

#' Get detailed information (e.g., name, OS, size, etc.) of a Virtual Machine.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureListVM
#' @inheritParams azureStartVM
#' @param ignore ignore
#'
#' @family Virtual machine functions
#' @export
azureVMInfo <- function(azureActiveContext, resourceGroup, vmName, subscriptionID,
                        ignore = "N", verbose = FALSE) {
  assert_that(is.azureActiveContext(azureActiveContext))

  if (missing(subscriptionID)) subscriptionID <- azureActiveContext$subscriptionID
  if (missing(resourceGroup)) resourceGroup <- azureActiveContext$resourceGroup
  if (missing(vmName)) vmName <- azureActiveContext$vmName
     
  assert_that(is_resource_group(resourceGroup))
  assert_that(is_subscription_id(subscriptionID))
  assert_that(is_vm_name(vmName))

  uri <- paste0("https://management.azure.com/subscriptions/", subscriptionID,
               "/resourceGroups/", resourceGroup, 
               "/providers/Microsoft.Compute/virtualmachines/", vmName, 
               "?$expand=instanceView&api-version=2015-05-01-preview")
  
  r <- call_azure_sm(azureActiveContext, uri = uri,
    verb = "GET", verbose = verbose)
  if(status_code(r) == 404 && ignore == "Y") return("NA")
  stopWithAzureError(r)

  rl <- content(r, "text", encoding = "UTF-8")
  df <- fromJSON(rl)

  dfn <- df$properties$instanceView$statuses
  
  clust <- nrow(dfn)
  if (clust < 1) {
    if (ignore == "Y") {
      return("NA")
    } else {
      stop("No Virtual Machines found")
    }
  }
  
  return(list(vmName = df$properties$osProfile$computerName,
              vmId = df$id,
              userName = df$properties$osProfile$computerName,
              os = df$properties$storageProfile$osDisk$osType,
              size = df$properties$hardwareProfile$vmSize,
              location = df$location,
              status = paste(dfn$displayStatus, collapse = ", ")))
}
