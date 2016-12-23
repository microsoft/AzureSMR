#' List VMs in a Subscription.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureListAllResources
#'
#' @family Virtual machine functions
#' @export
azureListVM <- function(azureActiveContext, resourceGroup, location, subscriptionID,
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

  URL <- paste("https://management.azure.com/subscriptions/", SUBIDI,
               "/resourceGroups/", RGI, "/providers/Microsoft.Compute/virtualmachines?api-version=2015-05-01-preview",
               sep = "")

  r <- GET(URL, add_headers(.headers = c(Host = "management.azure.com",
                                         Authorization = AT, `Content-type` = "application/json")), verbosity)
  rl <- content(r, "text", encoding = "UTF-8")
  df <- fromJSON(rl)
  #print(df)
  dfn <- as.data.frame(df$value$name)
  clust <- nrow(dfn)
  if (clust < 1) {
    warning("No Virtual Machines found")
    return(NULL)
  }
  dfn[1:clust, 1] <- df$value$name
  dfn[1:clust, 2] <- df$value$location
  dfn[1:clust, 3] <- df$value$type
  if(!is.null(df$value$properties$storageProfile$osDisk$osType))
    dfn[1:clust, 4] <- df$value$properties$storageProfile$osDisk$osType
  else
    dfn[1:clust, 4] <- "-"
  dfn[1:clust, 5] <- df$value$properties$provisioningState
  dfn[1:clust, 6] <- df$value$properties$osProfile$adminUsername
  dfn[1:clust, 7] <- df$value$id
  # dfn[1:clust,8] <- df$value$properties.vmsize
  # print(df$value$properties.vmsize)

  # dvn

  colnames(dfn) <- c("name", "location", "type", "OS", "state", "admin",
                     "ID")
  azureActiveContext$subscriptionID <- SUBIDI
  azureActiveContext$resourceGroup <- RGI

  return(dfn)
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
  if (missing(vmName)) {
    vmNameI <- azureActiveContext$vmNameI
  } else (vmNameI <- vmName)
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
  if (!length(vmNameI)) {
    stop("No VM name provided")
  }

  URL <- paste("https://management.azure.com/subscriptions/", SUBIDI,
               "/resourceGroups/", RGI, "/providers/Microsoft.Compute/virtualmachines/",
               vmNameI, "/start?api-version=2015-05-01-preview", sep = "")
  # print(URL)

  r <- POST(URL, add_headers(.headers = c(Host = "management.azure.com",
                                          Authorization = AT, `Content-type` = "application/json")), verbosity)
  if (status_code(r) == 404) {
    stop(paste("Error: Return code", status_code(r)), " (Not Found)")
  }
  if (status_code(r) != 200 && status_code(r) != 202)
    stopWithAzureError(r)
  rl <- content(r, "text", encoding = "UTF-8")

  # print(rl) df <- fromJSON(rl) dfn <- as.data.frame(df$value$name)
  azureActiveContext$subscriptionID <- SUBIDI
  azureActiveContext$resourceGroup <- RGI
  azureActiveContext$vmName <- vmNameI
  if (toupper(mode) == "SYNC") {
    rc <- "running"
    writeLines(paste("azureStartVM: Request Submitted: ", Sys.time()))
    writeLines("Updating(U), deallocating(D), starting(S), Deallocated(-) ")
    a <- 1
    Sys.sleep(5)
    while (a > 0) {
      rc1 <- azureVMStatus(azureActiveContext)
      # rc1 <- rc$displayStatus[2] print(rc) print(rc1)
      a <- a + 1
      if (grepl("running", rc1)) {
        writeLines("")
        writeLines(paste("Finished Started Sucessfully: ", Sys.time()))
        (break)()
      }
      if (grepl("Updating", rc1)) {
        rc1 <- "U"
      } else if (grepl("deallocating", rc1)) {
        rc1 <- "D"
      } else if (grepl("running", rc1)) {
        rc1 <- "R"
      } else if (grepl("starting", rc1)) {
        rc1 <- "S"
      } else if (grepl("updating", rc1)) {
        rc1 <- "U"
      } else if (grepl("deallocated", rc1)) {
        rc1 <- "-"
      }

      if (is.na(rc1))
        rc1 <- "."
      if (rc1 == "NA")
        rc1 <- "*"

      cat(rc1)

      if (a > 500)
        (break)()
      Sys.sleep(5)
    }
    writeLines(paste("Finished: ", Sys.time()))
    return("Done")
  }
  writeLines(paste("Start request Submitted: ", Sys.time()))
  return("")
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
  if (missing(vmName)) {
    vmNameI <- azureActiveContext$vmName
  } else (vmNameI <- vmName)
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
  if (!length(vmNameI)) {
    stop("No VM name provided")
  }

  URL <- paste("https://management.azure.com/subscriptions/", SUBIDI,
               "/resourceGroups/", RGI, "/providers/Microsoft.Compute/virtualmachines/",
               vmNameI, "/deallocate?api-version=2015-05-01-preview", sep = "")
  # print(URL)

  r <- POST(URL, add_headers(.headers = c(Host = "management.azure.com",
                                          Authorization = AT, `Content-type` = "application/json")), verbosity)
  if (status_code(r) == 404) {
    stop(paste("Error: Return code", status_code(r)), " (Not Found)")
  }
  if (status_code(r) != 200 && status_code(r) != 202)
    stopWithAzureError(r)

  rl <- content(r, "text", encoding = "UTF-8")
  # df <- fromJSON(rl)

  # dfn <- as.data.frame(df$value$name)
  azureActiveContext$subscriptionID <- SUBIDI
  azureActiveContext$resourceGroup <- RGI
  azureActiveContext$vmName <- vmNameI

  if (toupper(mode) == "SYNC") {
    rc <- "running"
    writeLines(paste("azureStopVM: Request Submitted: ", Sys.time()))
    writeLines("Updating(U), deallocating(D), starting(S), Stopped/Deallocated(-) ")
    a <- 1
    while (a > 0) {
      rc1 <- azureVMStatus(azureActiveContext)
      if (grepl("deallocated", rc1)) {
        writeLines("")
        writeLines(paste("Finished Deallocated Sucessfully: ",
                         Sys.time()))
        (break)()
      }

      a <- a + 1
      if (grepl("deallocating", rc1)) {
        rc1 <- "D"
      } else if (grepl("running", rc1)) {
        rc1 <- "R"
      } else if (grepl("updating", rc1)) {
        rc1 <- "U"
      } else if (grepl("Starting", rc1)) {
        rc1 <- "S"
      } else if (grepl("Stopped", rc1)) {
        rc1 <- "-"
      } else if (grepl("deallocated", rc1)) {
        rc1 <- "-"
      }

      cat(rc1)

      if (a > 500)
        (break)()
      Sys.sleep(5)
    }
    writeLines(paste("Finished: ", Sys.time()))
    return("Done")
  }
  writeLines(paste("Stop request Submitted: ", Sys.time()))
  return("")
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
                          azToken, ignore = "N", verbose = FALSE) {
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
  if (missing(vmName)) {
    vmNameI <- azureActiveContext$vmName
  } else (vmNameI <- vmName)
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
  if (!length(vmNameI)) {
    stop("No VM name provided")
  }

  URL <- paste("https://management.azure.com/subscriptions/", SUBIDI,
               "/resourceGroups/", RGI, "/providers/Microsoft.Compute/virtualmachines/",
               vmNameI, "/InstanceView?api-version=2015-05-01-preview", sep = "")
  # print(URL)

  r <- GET(URL, add_headers(.headers = c(Host = "management.azure.com",
                                         Authorization = AT, `Content-type` = "application/json")), verbosity)
  rl <- content(r, "text", encoding = "UTF-8")
  df <- fromJSON(rl)
  # print(df)
  if (length(df$error$code) && df$error$code == "ExpiredAuthenticationToken")
    stop("Authentication token has expired. Run azureAuthenticate() to renew.")

  dfn <- as.data.frame(df$statuses)

  clust <- nrow(dfn)
  if (clust < 1 && ignore == "Y")
    return("NA")
  if (clust < 1)
    stop("No Virtual Machines found")
  return(paste(df$statuses$displayStatus, collapse = ", "))

  return("Submitted")
}


#' Delete a Virtual Machine.
#'
#' @inheritParams azureListVM
#' @inheritParams azureStartVM
#' @family Virtual machine functions
#' @export
azureDeleteVM <- function(azureActiveContext, resourceGroup, vmName, subscriptionID,
                          azToken, mode = "Sync", verbose = FALSE) {
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
  if (missing(vmName)) {
    vmNameI <- azureActiveContext$vmNameI
  } else (vmNameI <- vmName)
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
  if (!length(vmNameI)) {
    stop("No VM name provided")
  }

  URL <- paste("https://management.azure.com/subscriptions/", SUBIDI,
               "/resourceGroups/", RGI, "/providers/Microsoft.Compute/virtualmachines/",
               vmNameI, "?api-version=2015-05-01-preview", sep = "")
  # print(URL)
  print(URL)

  r <- DELETE(URL, add_headers(.headers = c(Host = "management.azure.com",
                                            Authorization = AT, `Content-type` = "application/json")), verbosity)
  if (status_code(r) == 404) {
    stop(paste("Error: Return code", status_code(r)), " (Not Found)")
  }
  if (status_code(r) != 200 && status_code(r) != 202)
    stopWithAzureError(r)

  rl <- content(r, "text", encoding = "UTF-8")
  print(rl)
  # df <- fromJSON(rl)

  # dfn <- as.data.frame(df$value$name)
  azureActiveContext$subscriptionID <- SUBIDI
  azureActiveContext$resourceGroup <- RGI
  azureActiveContext$vmName <- vmNameI

  if (toupper(mode) == "SYNC") {
    rc <- "running"
    writeLines(paste("azureDeleteVM: Request Submitted: ", Sys.time()))
    writeLines("Updating(U), Deleting(D), Stopped/Deallocated(-) ")
    a <- 1
    while (a > 0) {
      rc <- azureVMStatus(ignore = "Y")
      if (grepl("NA", rc)) {
        writeLines("")
        writeLines(paste("Finished Deleted Sucessfully: ", Sys.time()))
        (break)()
      }

      a <- a + 1
      if (grepl("Deleting", rc)) {
        rc <- "D"
      } else if (grepl("running", rc)) {
        rc <- "R"
      } else if (grepl("updating", rc)) {
        rc <- "U"
      } else if (grepl("Stopped", rc)) {
        rc <- "-"
      } else if (grepl("deallocated", rc)) {
        rc <- "-"
      }

      cat(rc)

      if (a > 500)
        (break)()
      Sys.sleep(5)
    }
  }
  writeLines(paste("Finished: ", Sys.time()))
  return(rc)
}

#' List scale sets within a resource group.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureListAllResources
#'
#' @family Virtual machine functions
#' @export
azureListScaleSets <- function(azureActiveContext, resourceGroup, location, subscriptionID,
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
  
  URL <- paste("https://management.azure.com/subscriptions/", SUBIDI,
               "/resourceGroups/", RGI, "/providers/Microsoft.Compute/virtualMachineScaleSets?api-version=2016-03-30",
               sep = "")
  
  r <- GET(URL, add_headers(.headers = c(Host = "management.azure.com",
                                         Authorization = AT, `Content-type` = "application/json")), verbosity)
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
  azureActiveContext$subscriptionID <- SUBIDI
  azureActiveContext$resourceGroup <- RGI
  
  return(dfn)
}

#' List scale set network information with a ResourceGroup
#'
#' @inheritParams setAzureContext
#' @inheritParams azureListAllResources
#'
#' @family Virtual machine functions
#' @export
azureListScaleSetNetwork <- function(azureActiveContext,resourceGroup, location, subscriptionID,
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
  
  URL <- paste("https://management.azure.com/subscriptions/", SUBIDI,
               "/resourceGroups/", RGI, "/providers/Microsoft.Network/loadBalancers/","?api-version=2016-03-30",
               sep = "")
  
  r <- GET(URL, add_headers(.headers = c(Host = "management.azure.com",
                                         Authorization = AT, `Content-type` = "application/json")), verbosity)
  rl <- content(r, "text", encoding = "UTF-8")
  df <- fromJSON(rl)
  lbs <- df$value$name
  dfn <- data.frame(lbname="",publicIpAdress="",inport="",outport="")
  clust <- length(lbs)
  if (clust > 0) 
  {
     for (lb in lbs)
     {
       URL <- paste("https://management.azure.com/subscriptions/", SUBIDI,
                    "/resourceGroups/", RGI, "/providers/Microsoft.Network/loadBalancers/",lb,"?api-version=2016-03-30",
                    sep = "")
       r <- GET(URL, add_headers(.headers = c(Host = "management.azure.com",
                                              Authorization = AT, `Content-type` = "application/json")), verbosity)
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
    colnames(dfn) <- c("lbname","domainName","publicIP", "inPort", "outPort")
  }
  
  URL <- paste("https://management.azure.com/subscriptions/", SUBIDI,
               "/resourceGroups/", RGI, "/providers/Microsoft.Network/publicIPAddresses","?api-version=2016-03-30",
               sep = "")
  
  r <- GET(URL, add_headers(.headers = c(Host = "management.azure.com",
                                         Authorization = AT, `Content-type` = "application/json")), verbosity)
  rl <- content(r, "text", encoding = "UTF-8")
  df <- fromJSON(rl)
  pips <- df$value$name
  clust <- length(lbs)
  
  if (clust > 0) 
  {
    for (pip in pips)
    {
      URL <- paste("https://management.azure.com/subscriptions/", SUBIDI,
                   "/resourceGroups/", RGI, "/providers/Microsoft.Network/publicIPAddresses/",pip,"?api-version=2016-03-30",
                   sep = "")
      r <- GET(URL, add_headers(.headers = c(Host = "management.azure.com",
                                             Authorization = AT, `Content-type` = "application/json")), verbosity)
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
  
  azureActiveContext$subscriptionID <- SUBIDI
  azureActiveContext$resourceGroup <- RGI
  
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
  verbosity <- if (verbose)
    httr::verbose(TRUE) else NULL
  
  if (!length(RGI)) {
    stop("Error: No resourceGroup provided: Use resourceGroup argument or set in AzureContext")
  }
  if (!length(scaleSet)) {
    stop("Error: No scaleSet provided")
  }
  if (!length(SUBIDI)) {
    stop("Error: No subscriptionID provided: Use SUBID argument or set in AzureContext")
  }
  if (!length(AT)) {
    stop("Error: No Token / Not currently Authenticated")
  }
  
  
  URL <- paste("https://management.azure.com/subscriptions/", SUBIDI,
               "/resourceGroups/", RGI, "/providers/Microsoft.Compute/virtualMachineScaleSets/",scaleSet,"/virtualMachines?api-version=2016-03-30",
               sep = "")
  
  r <- GET(URL, add_headers(.headers = c(Host = "management.azure.com",
                                         Authorization = AT, `Content-type` = "application/json")), verbosity)
  rl <- content(r, "text", encoding = "UTF-8")
  df <- fromJSON(rl)
  dfn <- as.data.frame(df$value$name)
  clust <- nrow(dfn)
  if (clust < 1) {
    dfn[1:clust, 1] <- ""
    dfn[1:clust, 2] <- ""
    dfn[1:clust, 3] <- ""
    dfn[1:clust, 4] <- ""
    colnames(dfn) <- c("name","id", "computerName", "state")
    print("No Virtual Machines scalesets found")
    return(dfn[-1,])
  }
  dfn[1:clust, 1] <- df$value$name
  dfn[1:clust, 2] <- df$value$instanceId
  dfn[1:clust, 3] <- df$value$properties$osProfile$computerName
  dfn[1:clust, 4] <- df$value$properties$provisioningState

  colnames(dfn) <- c("name","id", "computerName", "state")
  azureActiveContext$subscriptionID <- SUBIDI
  azureActiveContext$resourceGroup <- RGI
  
  return(dfn)
}

