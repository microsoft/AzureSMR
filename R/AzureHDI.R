#' Get all HDInsight Clusters in default Subscription or details for a specified cluster name.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @inheritParams azureListAllResources
#'
#' @family HDInsight functions
#' @references https://docs.microsoft.com/en-us/rest/api/hdinsight/hdinsight-cluster#list-by-subscription
#'
#' @return data frame with summary information of HDI clusters
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
  rc <- content(r)
  extract_one <- function(x) {
     as.data.frame(
          c(
            x[c("name", "id", "location", "type")],
            x$properties[c("tier", "osType", "provisioningState", "clusterState", "createdDate")],
            x$properties$clusterDefinition[c("kind")]
            ))
  }

  z <- if (is.null(rc$value)) {
    extract_one(rc)
  } else {
    do.call(rbind, lapply(rc$value, extract_one))
  }
  
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
  assert_that(is.azureActiveContext(azureActiveContext))
  azureCheckToken(azureActiveContext)
  azToken <- azureActiveContext$Token
  if (missing(subscriptionID)) subscriptionID <- azureActiveContext$subscriptionID
  if (missing(resourceGroup)) resourceGroup <- azureActiveContext$resourceGroup
  if (missing(clustername)) clustername <- azureActiveContext$clustername

  verbosity <- set_verbosity(verbose)
     
  assert_that(is_subscription_id(subscriptionID))
  assert_that(is_resource_group(resourceGroup))
  assert_that(is_clustername(clustername))

  URL <- paste0("https://management.azure.com/subscriptions/", subscriptionID,
               "/resourceGroups/", resourceGroup, 
               "/providers/Microsoft.HDInsight/clusters/", clustername, 
               "?api-version=2015-03-01-preview")

  r <- GET(URL, azureApiHeaders(azToken), verbosity)
  rc <- content(r)

  if (length(rc) == 0) {
    warning("No HDInsight clusters found", immediate. = TRUE)
  }

  info <- paste(vapply(rc$properties$computeProfile$roles, function(x) {
    sprintf("%s: %s * %s",
      x$name,
      x$targetInstanceCount,
      x$hardwareProfile$vmSize)}, FUN.VALUE = character(1)
    ), collapse = ", "
  )

  dfn <- with(rc, data.frame(
    name = name,
    id = id,
    location = location,
    type = type,
    tier = rc$properties$tier,
    kind = rc$properties$clusterDefinition$kind,
    osType = rc$properties$osType,
    provisioningState = rc$properties$provisioningState,
    status = rc$properties$clusterState,
    created = rc$properties$createdDate,
    numCores = rc$properties$quotaInfo$coresUsed,
    information = info,
    stringsAsFactors = FALSE
    ))

  return(dfn)
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
#' @param debug Used for debugging purposes. If TRUE, returns json without attempting to connect to Azure
#'
#' @return Success message
#' @family HDInsight functions
#' @note See \url{https://docs.microsoft.com/en-us/azure/hdinsight/hdinsight-component-versioning} to learn about HDInsight Versions
#' @export
azureCreateHDI <- function(azureActiveContext, resourceGroup, location,
                           clustername, kind = c("rserver", "spark", "hadoop"),
                           storageAccount, storageKey,
                           version = "3.5", componentVersion = "1.6.2",
                           workers = 2,
                           adminUser, adminPassword, sshUser, sshPassword,
                           hiveServer, hiveDB, hiveUser, hivePassword,
                           vmSize = "Large",
                           subscriptionID, mode = c("Sync", "Async"), 
                           verbose = FALSE, debug = FALSE) {
  assert_that(is.azureActiveContext(azureActiveContext))
  azureCheckToken(azureActiveContext)
  azToken <- azureActiveContext$Token

  kind <- match.arg(kind)
  mode <- match.arg(mode)

  if (missing(subscriptionID)) subscriptionID <- azureActiveContext$subscriptionID
  if (missing(resourceGroup)) resourceGroup <- azureActiveContext$resourceGroup
  if (missing(storageAccount)) storageAccount <- azureActiveContext$storageAccount
  verbosity <- set_verbosity(verbose)

  assert_that(is_resource_group(resourceGroup))
  if (missing(location)) {
    location <- getResourceGroupLocation(azureActiveContext, resourceGroup = resourceGroup)
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
  if (!storageAccount %in% storage_accounts$name) {
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
  if (mode == "Sync") {
    z <- pollStatusHDI(azureActiveContext, clustername = clustername)
    if (!z) return(FALSE)
    }
  azureActiveContext$hdiAdmin <- adminUser
  azureActiveContext$hdiPassword <- adminPassword
  azureActiveContext$clustername <- clustername
  message(paste("Finished: ", Sys.time()))
  return(TRUE)
}


#' Resize a HDInsight cluster role.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @inheritParams azureListHDI
#' @inheritParams azureCreateHDI
#'
#' @param role role type: 'worker', 'head' or 'edge'
#' @param size Numeric: the number of nodes for this type of role
#'
#' @family HDInsight functions
#' @export
azureResizeHDI <- function(azureActiveContext, clustername, 
                           role = c("worker", "head", "edge"),
                           size = 2, mode = c("Sync", "Async"), subscriptionID,
                           resourceGroup, verbose = FALSE) {
  azureCheckToken(azureActiveContext)
  azToken <- azureActiveContext$Token

  if (missing(resourceGroup)) resourceGroup <- azureActiveContext$resourceGroup
  if (missing(subscriptionID)) subscriptionID <- azureActiveContext$subscriptionID
  verbosity <- set_verbosity(verbose)
     
  assert_that(is_resource_group(resourceGroup))
  assert_that(is_clustername(clustername))
  assert_that(is.integer(size))

  role <- match.arg(role)
  mode <- match.arg(mode)
  verbosity <- set_verbosity(verbose)

  URL <- paste0("https://management.azure.com/subscriptions/", subscriptionID,
               "/resourceGroups/", resourceGroup, 
               "/providers/Microsoft.HDInsight/clusters/", clustername, 
               "/roles/", role, "/resize?api-version=2015-03-01-preview")

  bodyI <- list(targetInstanceCount = size)

  r <- POST(URL, azureApiHeaders(azToken),
            body = bodyI,
            encode = "json", verbosity)

  rl <- content(r, "text", encoding = "UTF-8")
  if (status_code(r) != 202) {
    stop(paste("Error: Return code", status_code(r)))
  }
  RT <- "Request accepted"
  if (mode == "Sync") {
    azureActiveContext$resourceGroup <- resourceGroup
    message(paste("azureResizeHDI: request submitted: ", Sys.time()))
    message("Key: A - accepted, (.) - in progress, S - succeeded")
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


#' Delete HDInsight cluster.
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

#' Run script action on HDI cluster.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @inheritParams azureListHDI
#' @inheritParams azureListVM
#'
#' @param scriptname Identifier for Custom action script operation
#' @param scriptURL URL to custom action script
#' @param headNode install on head nodes
#' @param workerNode install on worker nodes
#' @param edgeNode install on worker nodes
#' @param parameters parameters
#'
#' @return Returns Success Message
#' @family HDInsight functions
#' @export
azureRunScriptAction <- function(azureActiveContext, scriptname, scriptURL,
                                 headNode = TRUE, workerNode = FALSE, edgeNode = FALSE,
                                 clustername, resourceGroup,
                                 parameters = "", subscriptionID, verbose = FALSE) {
  assert_that(is.azureActiveContext(azureActiveContext))
  azureCheckToken(azureActiveContext)
  azToken <- azureActiveContext$Token

  if (missing(subscriptionID)) subscriptionID <- azureActiveContext$subscriptionID
  if (missing(resourceGroup)) resourceGroup <- azureActiveContext$resourceGroup
  if (missing(clustername)) clustername <- azureActiveContext$clustername
  verbosity <- set_verbosity(verbose)
     
  assert_that(is_resource_group(resourceGroup))
  assert_that(is_subscription_id(subscriptionID))
  assert_that(is_clustername(clustername))

  if (!length(scriptname)) {
    stop("Error: No Valid scriptname provided")
  }
  if (!length(scriptURL)) {
    stop("Error: No Valid scriptURL provided")
  }

  if (!any(headNode, workerNode, edgeNode)) {
    stop("Error: No role(headNode,workerNode,edgeNode) flag set to TRUE")
  }

  roles <- c(headNode = '"headnode"', 
             workerNode = '"workernode"',
             edgeNode = '"edgenode"')
  RL <- paste(roles[c(headNode, workerNode, edgeNode)], sep = ", ")
    
  bodyI <- paste0('
  {
    "scriptActions": [{
      "name":"', scriptname, '",
      "uri":"', scriptURL, '",
      "parameters":"', parameters, '",
      "roles":[', RL, ']
    }],
    "persistOnSuccess": true
  }')

  URL <- paste0("https://management.azure.com/subscriptions/", subscriptionID,
               "/resourceGroups/", resourceGroup, 
               "/providers/Microsoft.HDInsight/clusters/", clustername, 
               "/executeScriptActions?api-version=2015-03-01-preview")

  r <- POST(URL, azureApiHeaders(azToken),
            body = bodyI,
            encode = "json", verbosity)
  browser()
  stopWithAzureError(r)

  azureActiveContext$clustername <- clustername
  message("Accepted")
  return(TRUE)
}


#' Get all HDInsight script action history for a specified cluster name.
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
  assert_that(is.azureActiveContext(azureActiveContext))
  azureCheckToken(azureActiveContext)
  azToken <- azureActiveContext$Token

  if (missing(resourceGroup)) resourceGroup <- azureActiveContext$resourceGroup
  if (missing(subscriptionID)) subscriptionID <- azureActiveContext$subscriptionID
  if (missing(clustername)) clustername <- azureActiveContext$clustername
  verbosity <- set_verbosity(verbose)
     
  assert_that(is_resource_group(resourceGroup))
  assert_that(is_subscription_id(subscriptionID))
  assert_that(is_clustername(clustername))

  URL <- paste0("https://management.azure.com/subscriptions/", subscriptionID,
               "/resourceGroups/", resourceGroup, 
               "/providers/Microsoft.HDInsight/clusters/", clustername, 
               "/scriptExecutionHistory/?api-version=2015-03-01-preview")

  r <- GET(URL, azureApiHeaders(azToken), verbosity)
  stopWithAzureError(r)

  rc <- content(r)$value

  if (length(rc) == 0) {
    message("No script action history found")
  }

  dfn <- do.call(rbind, lapply(rc, function(x) {
    data.frame(
    x[c("name", "scriptExecutionId", "startTime")],
    if (is.null(x$endTime)) list(endTime = NA) else x["endTime"], 
    x[c("status", "uri", "parameters")]
    )
  }))

  azureActiveContext$clustername <- clustername
  return(dfn)
}

