#' Get all HDInsight Clusters in default Subscription or details for a specified cluster name.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @inheritParams azureListAllResources
#'
#' @family HDInsight functions
#'
#' @return Returns Dataframe of HDInsight Clusters
#' @export
azureListHDI <- function(azureActiveContext, resourceGroup, clustername = "*",
                         subscriptionID, name, type, location, verbose = FALSE) {

  assert_that(is.azureActiveContext(azureActiveContext))
  azureCheckToken(azureActiveContext)
  azToken <- azureActiveContext$Token
  if (missing(subscriptionID)) subscriptionID <- azureActiveContext$subscriptionID
  if (missing(resourceGroup)) resourceGroup <- azureActiveContext$resourceGroup
  verbosity <- set_verbosity(verbose)
     
  assert_that(is_subscription_id(subscriptionID))
  if (clustername != "*") {
    assert_that(is_clustername(clustername))
    assert_that(is_resource_group(resourceGroup))
  }

  rg <- if (clustername == "*") "" else paste0("/resourceGroups/", resourceGroup)
  cn <- if (clustername == "*") "" else clustername
 
  URL <- paste0("https://management.azure.com/subscriptions/", subscriptionID, rg,
           "/providers/Microsoft.HDInsight/clusters/", cn, 
           "?api-version=2015-03-01-preview")

  r <- GET(URL, azureApiHeaders(azToken), verbosity)
  stopWithAzureError(r)
 
  rc <- content(r)$value
  z <- do.call(rbind, lapply(rc, function(x) {
        as.data.frame(
          c(
            x[c("name", "id", "location", "type")],
            x$properties[c("tier", "osType", "provisioningState", "clusterState", "createdDate")],
            x$properties$clusterDefinition[c("kind")]
            )
            )
  }))
  
  azureActiveContext$resourceGroup <- resourceGroup

  return(z)
}


#' Get Configuration Information for a specified cluster name.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @inheritParams azureListHDI
#'
#'
#' @return Returns Dataframe of HDInsight Clusters information
#' @family HDInsight functions
#' @export
azureHDIConf <- function(azureActiveContext, clustername, resourceGroup,
                         subscriptionID, name, type, location, verbose = FALSE) {
  azureCheckToken(azureActiveContext)
  azToken <- azureActiveContext$Token
  if (missing(subscriptionID)) {
    subscriptionID <- azureActiveContext$subscriptionID
  } else (subscriptionID <- subscriptionID)
  if (missing(resourceGroup)) {
    resourceGroup <- azureActiveContext$resourceGroup
  } else (resourceGroup <- resourceGroup)
  if (missing(clustername)) {
    clustername <- azureActiveContext$clustername
  } else (clustername <- clustername)
  verbosity <- set_verbosity(verbose)
     

  if (!length(azToken)) {
    stop("Error: No Token / Not currently Authenticated.")
  }
  if (!length(subscriptionID)) {
    stop("Error: No subscriptionID provided: Use SUBID argument or set in AzureContext")
  }
  if (!length(clustername)) {
    stop("Error: No clustername Provided.")
  }

  URL <- paste("https://management.azure.com/subscriptions/", subscriptionID,
               "/resourceGroups/", resourceGroup, "/providers/Microsoft.HDInsight/clusters/",
               clustername, "?api-version=2015-03-01-preview", sep = "")

  r <- GET(URL, add_headers(.headers = c(Host = "management.azure.com",
                                         Authorization = azToken, `Content-type` = "application/json")), verbosity)
  rl <- content(r, "text")

  df <- fromJSON(rl)
  dfn <- as.data.frame(df$name)
  clust <- nrow(dfn)
  if (clust < 1) {
    warning("No HDInsight Clusters found")
    return(NULL)
  }

  dfn[1, 1] <- df$name
  dfn[1, 2] <- df$id
  dfn[1, 3] <- df$location
  dfn[1, 4] <- df$type
  dfn[1, 5] <- df$properties$tier
  dfn[1, 6] <- df$properties$clusterDefinition$kind
  dfn[1, 7] <- df$properties$osType
  dfn[1, 8] <- df$properties$provisioningState
  dfn[1, 9] <- df$properties$clusterState
  dfn[1, 10] <- df$properties$createdDate
  dfn[1, 11] <- df$properties$quotaInfo$coresUsed
  roles1 <- df$properties$computeProfile$roles
  rt <- ""
  for (i in 1:nrow(roles1)) {
    row <- roles1[i, ]
    rt <- paste(rt, row$name, "(", row$targetInstanceCount, "*", row[,
                                                                     3], ")")
  }
  dfn[1, 12] <- rt
  colnames(dfn) <- c("name", "ID", "location", "type", "tier", "kind",
                     "OS", "provState", "status", "created", "numCores", "information")

  return(t(dfn))
}


#' Resize a HDInsight Cluster role.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @inheritParams azureListHDI
#' @inheritParams azureCreateHDI
#'
#' @param role role type: 'worker', 'head' or 'Edge'
#' @param size Desired size of role type
#'
#' @family HDInsight functions
#' @export
azureResizeHDI <- function(azureActiveContext, clustername, role = "worker",
                           size = 2, mode = "Sync", subscriptionID,
                           resourceGroup, verbose = FALSE) {
  azureCheckToken(azureActiveContext)
  azToken <- azureActiveContext$Token

  if (missing(resourceGroup)) {
    resourceGroup <- azureActiveContext$resourceGroup
  } else (resourceGroup <- resourceGroup)
  if (missing(subscriptionID)) {
    subscriptionID <- azureActiveContext$subscriptionID
  } else (subscriptionID <- subscriptionID)
  verbosity <- set_verbosity(verbose)
     

  if (!length(resourceGroup)) {
    stop("Error: No resourceGroup provided: Use resourceGroup argument or set in AzureContext")
  }
  if (!length(clustername)) {
    stop("Error: No clustername provided")
  }
  if (!length(role)) {
    stop("Error: No role Provided")
  }
  if (!length(resourceGroup)) {
    stop("Error: No New role size provided")
  }
  verbosity <- set_verbosity(verbose)
     

  URL <- paste("https://management.azure.com/subscriptions/", subscriptionID,
               "/resourceGroups/", resourceGroup, "/providers/Microsoft.HDInsight/clusters/",
               clustername, "/roles/", role, "/resize?api-version=2015-03-01-preview",
               sep = "")

  bodyI <- list(targetInstanceCount = size)

  r <- POST(URL, add_headers(.headers = c(Host = "management.azure.com",
                                          Authorization = azToken,
                                          `Content-type` = "application/json")),
            body = bodyI,
            encode = "json", verbosity)

  rl <- content(r, "text", encoding = "UTF-8")
  if (status_code(r) != 202) {
    stop(paste("Error: Return code", status_code(r)))
  }
  RT <- "Request accepted"
  if (toupper(mode) == "SYNC") {
    azureActiveContext$resourceGroup <- resourceGroup
    message(paste("azureResizeHDI: Request Submitted: ", Sys.time()))
    message("Accepted(A), Resizing(R), Succeeded(S)")
    a <- 1
    while (a > 0) {
      rc <- azureListHDI(azureActiveContext, clustername = clustername)
      rc1 <- rc[9, 1]
      if (rc1 == "Running") {
        message("R")
        message("")
        message(paste("Finished Resizing Sucessfully: ", Sys.time()))
        (break)()
      }

      if (rc1 == "Error") {
        message("")
        message(paste("Error Resizing: ", Sys.time()))
        (break)()
      }

      a <- a + 1
      if (rc1 == "Accepted") {
        rc1 <- "A"
      }
      if (rc1 == "InProgress") {
        rc1 <- "R"
      }
      if (rc1 == "AzureVMConfiguration") {
        rc1 <- "R"
      }
      if (rc1 == "HdInsightConfiguration") {
        rc1 <- "R"
      }
      message(rc1)

      if (a > 500)
        (break)()
      Sys.sleep(5)
    }
    # RT <- clusters[12,1]
  }
  message(paste("Finished: ", Sys.time()))

  return(TRUE)
}


#' Delete Specifed HDInsight Cluster.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @inheritParams azureListHDI
#'
#' @return Returns Dataframe of HDInsight Clusters information
#' @family HDInsight functions
#' @export
azureDeleteHDI <- function(azureActiveContext, clustername, subscriptionID,
                           resourceGroup, verbose = FALSE) {

  assert_that(is.azureActiveContext(azureActiveContext))
  azureCheckToken(azureActiveContext)
  azToken <- azureActiveContext$Token

  assert_that(is_clustername(clustername))
  if (missing(subscriptionID)) subscriptionID <- azureActiveContext$subscriptionID
  if (missing(resourceGroup)) resourceGroup <- azureActiveContext$resourceGroup
  verbosity <- set_verbosity(verbose)
     
  assert_that(is_resource_group(resourceGroup))
  assert_that(is_clustername(clustername))

  URL <- paste0("https://management.azure.com/subscriptions/", subscriptionID,
               "/resourceGroups/", resourceGroup, "/providers/Microsoft.HDInsight/clusters/",
               clustername, "?api-version=2015-03-01-preview")

  r <- DELETE(URL, azureApiHeaders(azToken), verbosity)
  stopWithAzureError(r)

  message("Delete request accepted")
  return(TRUE)
}


#' Create HDInsight cluster.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @inheritParams azureListHDI
#'
#'
#' @param version HDinsight version
#' @param kind HDinsight kind: "hadoop","spark" or "rserver"
#' @param adminUser Admin user name
#' @param adminPassword Admin user password
#' @param workers Define the number of worker nodes
#' @param sshUser SSH user name
#' @param sshPassword SSH user password
#' @param hiveServer URI address of the Hive server
#' @param hiveDB Hive DB name
#' @param hiveUser Hive user name
#' @param hivePassword Hive user password
#' @param componentVersion Spark componentVersion. Default : 1.6.2
#' @param vmSize Size of nodes: "Large", "Small", "Standard_D14_V2", etc.
#' @param mode Provisioning mode, "Sync" or "Async". Use "Async" to immediately return to R session after submission of request
#'
#' @return Success message
#' @family HDInsight functions
#' @note See \url{https://docs.microsoft.com/en-us/azure/hdinsight/hdinsight-component-versioning} to learn about HDInsight Versions

#' @export
azureCreateHDI <- function(azureActiveContext, resourceGroup, location,
                           clustername, kind = c("spark", "hadoop", "rserver"),
                           storageAccount, storageKey, 
                           version = "3.5", componentVersion = "1.6.2", 
                           workers = 2,
                           adminUser, adminPassword, sshUser, sshPassword,
                           hiveServer, hiveDB, hiveUser, hivePassword,
                           vmSize = "Large",
                           subscriptionID, mode = "Sync", verbose = FALSE, debug = FALSE) {
  assert_that(is.azureActiveContext(azureActiveContext))
  azureCheckToken(azureActiveContext)
  azToken <- azureActiveContext$Token

  kind <- match.arg(kind)

  if (missing(subscriptionID)) subscriptionID <- azureActiveContext$subscriptionID
  if (missing(resourceGroup)) resourceGroup <- azureActiveContext$resourceGroup
  if (missing(storageAccount)) storageAccount <- azureActiveContext$storageAccount
  verbosity <- set_verbosity(verbose)

  assert_that(is_resource_group(resourceGroup))
  if (missing(location)) {
    location <- getResourceGroupLocation(azureActiveContext,
                                         resourceGroup = resourceGroup)
  }
  assert_that(is_location(location))
  assert_that(is_subscription_id(subscriptionID))

  assert_that(is_clustername(clustername))
  assert_that(is_storage_account(storageAccount))

  assert_that(is_admin_user(adminUser))
  assert_that(is_admin_password(adminPassword))
  assert_that(is_ssh_user(sshUser))
  assert_that(is_ssh_password(sshPassword))

  storage_accounts <- azureListSA(azureActiveContext)
  if(!storageAccount %in% storage_accounts$name) {
    # create storage account
    message("creating storage account: ", storageAccount)
    azureCreateStorageAccount(azureActiveContext, storageAccount = storageAccount, resourceGroup = resourceGroup, location = location)
    storageResGroup <- resourceGroup
  } else {
    # retrieve resource group of storage account
    idx <- storage_accounts$name == storageAccount
    storageResGroup <- storage_accounts$resourceGroup[idx]
  }

  storageKey <- azureSAGetKey(azureActiveContext, storageAccount = storageAccount, resourceGroup = storageResGroup)

  HIVE <- FALSE
  hivejson <- ""
  if (!missing(hiveServer)) {

    HIVE <- TRUE
    if (!length(hiveDB)) {
      stop("Error: hiveServer: No Valid hiveDB provided")
    }
    if (!length(hiveUser)) {
      stop("Error: hiveServer: No Valid hiveUser provided")
    }
    if (!length(hivePassword)) {
      stop("Error: hiveServer: No Valid hivePassword provided")
    }
    hivejson <- hive_json(hiveServer = hiveServer, hiveDB = hiveDB,
    hiveUser = hiveUser, hivePassword = hivePassword)
  }

  bodyI <- hdi_json(subscriptionID = subscriptionID, clustername = clustername,
    location = location, storageAccount = storageAccount, storageKey = storageKey,
    version = version,
    kind = kind, vmSize = vmSize,
    hivejson = hivejson,
    componentVersion = componentVersion,
    sshUser = sshUser, sshPassword = sshPassword, 
    adminUser = adminUser, adminPassword = adminPassword,
    workers = workers)

  if (debug) {
    z <- fromJSON(bodyI)
    return(z)
  }

  URL <- paste0("https://management.azure.com/subscriptions/", subscriptionID,
               "/resourceGroups/", resourceGroup,
               "/providers/Microsoft.HDInsight/clusters/", clustername,
               "?api-version=2015-03-01-preview")

  r <- PUT(URL, azureApiHeaders(azToken), body = bodyI, encode = "json", verbosity)
  #browser()
  stopWithAzureError(r)

  azureActiveContext$resourceGroup <- resourceGroup
  rl <- content(r, "text", encoding = "UTF-8")
  if (toupper(mode) == "SYNC") {
    z <- pollStatusHDI(azureActiveContext, clustername = clustername)
    if(!z) return(FALSE)
  }
  azureActiveContext$hdiAdmin <- adminUser
  azureActiveContext$hdiPassword  <- adminPassword
  azureActiveContext$clustername <- clustername
  message(paste("Finished: ", Sys.time()))
  return(TRUE)
}


#' Run Script Action on HDI Cluster.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @inheritParams azureListHDI
#' @inheritParams azureListVM
#'
#' @param scriptname Identifier for Custom action scrript operation
#' @param scriptURL URL to custom action script (Sring)
# @param roles - Specificy the roles to apply action string
# (workernode,headnode,edgenode)
#' @param headNode install on head nodes (default FALSE)
#' @param workerNode install on worker nodes (default FALSE)
#' @param edgeNode install on worker nodes (default FALSE)
#' @param parameters parameters
#'
#' @return Returns Success Message
#' @family HDInsight functions
#' @export
azureRunScriptAction <- function(azureActiveContext, scriptname = "script1", scriptURL,
                                 headNode = TRUE, workerNode = FALSE, edgeNode = FALSE,
                                 clustername, resourceGroup,
                                 parameters = "", subscriptionID, verbose = FALSE) {
  azureCheckToken(azureActiveContext)
  azToken <- azureActiveContext$Token

  if (missing(clustername)) {
    clustername <- azureActiveContext$clustername
  } else (clustername <- clustername)
  if (missing(resourceGroup)) {
    resourceGroup <- azureActiveContext$resourceGroup
  } else (resourceGroup <- resourceGroup)
  if (missing(subscriptionID)) {
    subscriptionID <- azureActiveContext$subscriptionID
  } else (subscriptionID <- subscriptionID)
  verbosity <- set_verbosity(verbose)
     

  if (!length(clustername)) {
    stop("Error: No Valid clustername provided")
  }
  if (!length(resourceGroup)) {
    stop("Error: No resourceGroup provided: Use resourceGroup argument or set in AzureContext")
  }
  if (!length(scriptname)) {
    stop("Error: No Valid scriptname provided")
  }
  if (!length(scriptURL)) {
    stop("Error: No Valid scriptURL provided")
  }

  RL <- ""
  if (headNode == TRUE)
    RL <= "\"headnode\""

  if (headNode == TRUE)
    RL <- "\"headnode\""

  if (workerNode == TRUE)
    if (nchar(RL) == 0)
      RL <- "\"workernode\"" else RL <- paste0(RL, ",\"workernode\"")
  if (edgeNode == TRUE)
    if (nchar(RL) == 0)
      RL <- "\"edgenode\"" else RL <- paste0(RL, ",\"edgenode\"")

  if (nchar(RL) == 0)
    stop("Error: No role(headNode,workerNode,edgeNode) flag set to TRUE")
  bodyI <- "
  {
  \"scriptActions\": [
  {
  \"name\": \"NNNNNNNNNN\",
  \"uri\": \"UUUUUUUUUUU\",
  \"parameters\": \"PPPPPPPPPPPP\",
  \"roles\": [RRRRRRRRRRRRRR]
  }
  ],
  \"persistOnSuccess\": true
  }"
  bodyI <- gsub("NNNNNNNNNN", scriptname, bodyI)
  bodyI <- gsub("UUUUUUUUUUU", scriptURL, bodyI)
  bodyI <- gsub("PPPPPPPPPPPP", parameters, bodyI)
  bodyI <- gsub("RRRRRRRRRRRRRR", RL, bodyI)


  URL <- paste("https://management.azure.com/subscriptions/", subscriptionID,
               "/resourceGroups/", resourceGroup, "/providers/Microsoft.HDInsight/clusters/",
               clustername, "/executeScriptActions?api-version=2015-03-01-preview", sep = "")

  r <- POST(URL, add_headers(.headers = c(Host = "management.azure.com",
                                          Authorization = azToken,
                                          `Content-type` = "application/json")),
            body = bodyI,
            encode = "json", verbosity)

  rl <- content(r, "text", encoding = "UTF-8")

  if (status_code(r) == 409) {
    stop(paste("Error: Conflict(Action script in progress on cluster?)",
               status_code(r)))
  }

  if (status_code(r) != 202) {
    stop(paste("Error: Return code", status_code(r)))
  }

  azureActiveContext$clustername <- clustername
  message("Accepted")
  return(TRUE)
}


#' Get all HDInsight Script Action History for a specified cluster name.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureListHDI
#' @inheritParams azureRunScriptAction
#'
#' @return Dataframe of HDInsight Clusters
#' @family HDInsight functions
#' @export
azureScriptActionHistory <- function(azureActiveContext, resourceGroup,
                                     clustername = "*", subscriptionID, 
                                     name, type, verbose = FALSE) {
  azureCheckToken(azureActiveContext)
  azToken <- azureActiveContext$Token

  if (missing(clustername)) {
    clustername <- azureActiveContext$clustername
  } else (clustername <- clustername)
  if (missing(subscriptionID)) {
    subscriptionID <- azureActiveContext$subscriptionID
  } else (subscriptionID <- subscriptionID)
  if (missing(resourceGroup)) {
    resourceGroup <- azureActiveContext$resourceGroup
  } else (resourceGroup <- resourceGroup)
  verbosity <- set_verbosity(verbose)
     
  if (!length(subscriptionID)) {
    stop("Error: No subscriptionID provided: Use SUBID argument or set in AzureContext")
  }
  if (clustername != "*" && !length(resourceGroup)) {
    stop("Error: No resourceGroup Defined.")
  }

  # https://management.azure.com/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupname}/providers/Microsoft.HDInsight/clusters/{clustername}/scriptExecutionHistory/{scriptExecutionId}?api-version={api-version}

  URL <- paste("https://management.azure.com/subscriptions/", subscriptionID,
               "/resourceGroups/", resourceGroup, "/providers/Microsoft.HDInsight/clusters/",
               clustername, "/scriptExecutionHistory/?api-version=2015-03-01-preview",
               sep = "")

  r <- GET(URL, add_headers(.headers = c(Host = "management.azure.com",
                                         Authorization = azToken,
                                         `Content-type` = "application/json")),
           verbosity)
  rl <- content(r, "text")
  df <- fromJSON(rl)
  dfn <- as.data.frame(df$value$scriptExecutionId)
  Hist <- nrow(dfn)

  if (Hist < 1) {
    warning("No ScriptAction history found")
    return(NULL)
  }

  dfn[1:Hist, 1] <- df$value$name
  dfn[1:Hist, 2] <- df$value$scriptExecutionId
  dfn[1:Hist, 3] <- paste0(df$value$roles)
  dfn[1:Hist, 4] <- df$value$startTime
  dfn[1:Hist, 5] <- df$value$endTime

  dfn[1:Hist, 6] <- df$value$status
  dfn[1:Hist, 7] <- df$value$uri
  dfn[1:Hist, 8] <- df$value$parameters

  colnames(dfn) <- c("name", "ID", "roles", "startTime", "endTime", "status",
                     "uri", "parameters")
  azureActiveContext$clustername <- clustername

  return(dfn)
}

