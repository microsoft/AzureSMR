#' Get all HDInsight Clusters in default Subscription or details for a specified clustername.
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
                         subscriptionID, azToken, name, type, location, verbose = FALSE) {

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

  if (!length(AT)) {
    stop("Error: No Token / Not currently Authenticated.")
  }
  if (!length(SUBIDI)) {
    stop("Error: No subscriptionID provided: Use SUBID argument or set in AzureContext")
  }
  if (clustername != "*" && !length(RGI)) {
    stop("Error: No resourceGroup Defined.")
  }

  URL <- if (clustername == "*") {
    paste0("https://management.azure.com/subscriptions/", SUBIDI,
           "/providers/Microsoft.HDInsight/clusters?api-version=2015-03-01-preview")
  } else {
    paste0("https://management.azure.com/subscriptions/", SUBIDI,
           "/resourceGroups/", RGI, "/providers/Microsoft.HDInsight/clusters/",
           clustername, "?api-version=2015-03-01-preview")
  }

  r <- GET(URL, add_headers(.headers = c(Host = "management.azure.com",
                                         Authorization = AT,
                                         `Content-type` = "application/json")),
           verbosity)
  rl <- content(r, "text")
  df <- fromJSON(rl)
#  print(df)
  if (clustername == "*") {
    dfn <- as.data.frame(df$value$name)
    clust <- nrow(dfn)
    if (clust < 1) {
      warning("No HDInsight Clusters found.")
      return(NULL)
    }
    dfn[1:clust, 1] <- df$value$name
    dfn[1:clust, 2] <- df$value$id
    dfn[1:clust, 3] <- df$value$location
    dfn[1:clust, 4] <- df$value$type
    dfn[1:clust, 5] <- df$value$properties$tier
    dfn[1:clust, 6] <- df$value$properties$clusterDefinition$kind
    if(!is.null(df$value$properties$osType))
          dfn[1:clust, 7] <- df$value$properties$osType
    else
      dfn[1:clust, 7] <- "-"
    dfn[1:clust, 8] <- df$value$properties$provisioningState
    dfn[1:clust, 9] <- df$value$properties$clusterState
    dfn[1:clust, 10] <- df$value$properties$createdDate
    dfn[1:clust, 11] <- df$value$properties$quotaInfo$coresUsed

    roles1 <- df$value$properties$computeProfile$roles
    j <- 1
    for (roles in roles1) {
      rt <- ""
      for (i in 1:nrow(roles)) {
        row <- roles[i, ]
        rt <- paste(rt, row$name, "(", row$targetInstanceCount,
                    "*", row[, 3], ")")
      }
      dfn[j, 12] <- rt
      j <- j + 1
    }
  } else {

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
      rt <- paste(rt, row$name, "(", row$targetInstanceCount, "*",
                  row[, 3], ")")
    }
    dfn[1, 12] <- rt
  }
  colnames(dfn) <- c("name", "ID", "location", "type", "tier", "kind",
                     "OS", "provState", "status", "created", "numCores", "information")
  azureActiveContext$resourceGroup <- RGI

  return(t(dfn))
}


#' Get Configuration Information for a specified clustername.
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
                         subscriptionID, azToken, name, type, location, verbose = FALSE) {
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
  if (missing(clustername)) {
    CN <- azureActiveContext$clustername
  } else (CN <- clustername)
  verbosity <- if (verbose)
    httr::verbose(TRUE) else NULL

  if (!length(AT)) {
    stop("Error: No Token / Not currently Authenticated.")
  }
  if (!length(SUBIDI)) {
    stop("Error: No subscriptionID provided: Use SUBID argument or set in AzureContext")
  }
  if (!length(CN)) {
    stop("Error: No clustername Provided.")
  }

  URL <- paste("https://management.azure.com/subscriptions/", SUBIDI,
               "/resourceGroups/", RGI, "/providers/Microsoft.HDInsight/clusters/",
               clustername, "?api-version=2015-03-01-preview", sep = "")

  r <- GET(URL, add_headers(.headers = c(Host = "management.azure.com",
                                         Authorization = AT, `Content-type` = "application/json")), verbosity)
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
                           size = 2, mode = "Sync", azToken, subscriptionID,
                           resourceGroup, verbose = FALSE) {
  azureCheckToken(azureActiveContext)

  if (missing(resourceGroup)) {
    RGI <- azureActiveContext$resourceGroup
  } else (RGI <- resourceGroup)
  if (missing(subscriptionID)) {
    SUBIDI <- azureActiveContext$subscriptionID
  } else (SUBIDI <- subscriptionID)
  if (missing(azToken)) {
    AT <- azureActiveContext$Token
  } else (AT <- azToken)
  verbosity <- if (verbose)
    httr::verbose(TRUE) else NULL

  if (!length(RGI)) {
    stop("Error: No resourceGroup provided: Use resourceGroup argument or set in AzureContext")
  }
  if (!length(clustername)) {
    stop("Error: No clustername provided")
  }
  if (!length(role)) {
    stop("Error: No role Provided")
  }
  if (!length(RGI)) {
    stop("Error: No New role size provided")
  }
  verbosity <- if (verbose)
    httr::verbose(TRUE) else NULL

  URL <- paste("https://management.azure.com/subscriptions/", SUBIDI,
               "/resourceGroups/", RGI, "/providers/Microsoft.HDInsight/clusters/",
               clustername, "/roles/", role, "/resize?api-version=2015-03-01-preview",
               sep = "")

  bodyI <- list(targetInstanceCount = size)

  r <- POST(URL, add_headers(.headers = c(Host = "management.azure.com",
                                          Authorization = AT,
                                          `Content-type` = "application/json")),
            body = bodyI,
            encode = "json", verbosity)

  rl <- content(r, "text", encoding = "UTF-8")
  if (status_code(r) != 202) {
    stop(paste("Error: Return code", status_code(r)))
  }
  RT <- "Request accepted"
  if (toupper(mode) == "SYNC") {
    azureActiveContext$resourceGroup <- RGI
    writeLines(paste("azureResizeHDI: Request Submitted: ", Sys.time()))
    writeLines("Accepted(A), Resizing(R), Succeeded(S)")
    a <- 1
    while (a > 0) {
      rc <- azureListHDI(azureActiveContext, clustername = clustername)
      rc1 <- rc[9, 1]
      if (rc1 == "Running") {
        cat("R")
        writeLines("")
        writeLines(paste("Finished Resizing Sucessfully: ", Sys.time()))
        (break)()
      }

      if (rc1 == "Error") {
        writeLines("")
        writeLines(paste("Error Resizing: ", Sys.time()))
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
      cat(rc1)

      if (a > 500)
        (break)()
      Sys.sleep(5)
    }
    # RT <- clusters[12,1]
  }
  writeLines(paste("Finished: ", Sys.time()))

  return("Done")
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
azureDeleteHDI <- function(azureActiveContext, clustername, azToken, subscriptionID,
                           resourceGroup, verbose = FALSE) {

  azureCheckToken(azureActiveContext)
  if (missing(clustername)) {
    CN <- azureActiveContext$clustername
  } else (CN <- clustername)
  if (missing(resourceGroup)) {
    RGI <- azureActiveContext$resourceGroup
  } else (RGI <- resourceGroup)
  if (missing(subscriptionID)) {
    SUBIDI <- azureActiveContext$subscriptionID
  } else (SUBIDI <- subscriptionID)
  if (missing(azToken)) {
    ATI <- azureActiveContext$Token
  } else (ATI <- azToken)
  verbosity <- if (verbose)
    httr::verbose(TRUE) else NULL

  if (!length(CN)) {
    stop("Error: No Valid clustername provided")
  }
  if (!length(RGI)) {
    stop("Error: No resourceGroup provided: Use resourceGroup argument or set in AzureContext")
  }

  URL <- paste("https://management.azure.com/subscriptions/", SUBIDI,
               "/resourceGroups/", RGI, "/providers/Microsoft.HDInsight/clusters/",
               CN, "?api-version=2015-03-01-preview", sep = "")


  r <- DELETE(URL, add_headers(.headers = c(Host = "management.azure.com",
                                            Authorization = ATI,
                                            `Content-type` = "application/json")),
              verbosity)
  if (status_code(r) != 202) {
    stop(paste("Error: Return code", status_code(r)))
  }

  rl <- content(r, "text", encoding = "UTF-8")

  return("Delete Request Accepted")
}


#' Create Specifed HDInsight Cluster.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @inheritParams azureListHDI
#'
#'
#' @param version HDinsight version
#' @param adminUser Admin user name
#' @param adminPassword Admin user password
#' @param workers Define the number of worker nodes
#' @param sshUser SSH user name
#' @param sshPassword SSH user password
#' @param hiveServer URI address of the Hive server
#' @param hiveDB Hive DB name
#' @param hiveUser Hive user name
#' @param hivePassword Hive user password
#' @param mode Provisioning mode, "Sync" or "Async". Use "Async" to immediately return to R session after submission of request
#'
#' @return Success message
#' @family HDInsight functions
#' @note See \url{https://docs.microsoft.com/en-us/azure/hdinsight/hdinsight-component-versioning} to learn about HDInsight Versions

#' @export
azureCreateHDI <- function(azureActiveContext, clustername, location, kind = "spark", storageAccount,
                           storageKey, version = "3.4", workers = 2, adminUser, adminPassword, sshUser,
                           sshPassword, hiveServer, hiveDB, hiveUser, hivePassword, resourceGroup,
                           azToken, subscriptionID, mode = "Sync", verbose = FALSE) {
  azureCheckToken(azureActiveContext)

  if (missing(resourceGroup)) {
    RGI <- azureActiveContext$resourceGroup
  } else (RGI <- resourceGroup)
  if (missing(subscriptionID)) {
    SUBIDI <- azureActiveContext$subscriptionID
  } else (SUBIDI <- subscriptionID)
  if (missing(azToken)) {
    ATI <- azureActiveContext$Token
  } else (ATI <- azToken)
  verbosity <- if (verbose) httr::verbose(TRUE) else NULL

  if (!length(storageAccount)) {
    stop("Error: No Storage Account (StorageAcc) provided")
  }
  # if (!length(storageKey)) {stop('Error: No Storage Key (storageKey) provided')}
  if (!length(location)) {
    stop("Error: No location provided")
  }
  if (!length(clustername)) {
    stop("Error: No Valid clustername provided")
  }
  if (!length(sshUser)) {
    stop("Error: No Valid sshUser provided")
  }
  if (!length(sshPassword)) {
    stop("Error: No Valid sshPassword provided")
  }
  if (!length(adminUser)) {
    stop("Error: No Valid adminUser provided")
  }
  if (!length(adminPassword)) {
    stop("Error: No Valid adminPassword provided")
  }
  if (!length(RGI)) {
    stop("Error: No resourceGroup provided: Use resourceGroup argument or set in AzureContext")
  }

  cat("Fetching Storage Key..")
  storageKey <- azureSAGetKey(azureActiveContext, resourceGroup = RGI, storageAccount = storageAccount)

  HIVE <- FALSE
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
  }

  msg <- "The Admin Password must be greater than 6 characters and contain at least one uppercase char, one lowercase char and one digit"
  if ((!grepl("[A-Z]", adminPassword))) warning(msg)
  if ((!grepl("[a-z]", adminPassword))) warning(msg)
  if ((!grepl("[0-9]", adminPassword))) warning(msg)
  msg <- "The SSH Password must be greater than 6 characters and contain at least one uppercase char, one lowercase char and one digit"
  if ((!grepl("[A-Z]", sshPassword))) warning(msg)
  if ((!grepl("[a-z]", sshPassword))) warning(msg)
  if ((!grepl("[0-9]", sshPassword))) warning(msg)

  SauthKey <- "oNgZByVwfm2V1CslTHe28e4LJXeY+JjP2f9RLIOg89g2q696EgTRhJkQpHCkOx1Wg0JvowraTgHxT1uKEHM2hA=="

  bodyI = '
  {
    "id":"/subscriptions/SSSSSSSSSSSS/resourceGroups/Analytics/providers/Microsoft.HDInsight/clusters/CCCCCC",
  "name":"CCCCCC",
  "type":"Microsoft.HDInsight/clusters",
  "location": "LLLLLLLLLLL",
  "tags": { "tag1": "value1", "tag2": "value2" },
  "properties": {
  "clusterversion": "VVVV",
  "osType": "Linux",
  "tier": "standard",
  "clusterDefinition": {
  "kind": "DDDDDDDDD",
  "configurations": {
  "gateway": {
  "restAuthCredential.isEnabled": true,
  "restAuthCredential.username": "AAAAAAAAAAA",
  "restAuthCredential.password": "QQQQQQQQQQQ"
  },
  "core-site": {
  "fs.defaultFS": "wasb://CCCCCC@TTTTTTTTTTT.blob.core.windows.net",
  "fs.azure.account.key.TTTTTTTTTTT.blob.core.windows.net": "KKKKKKKKKKKKKKKK"
  }
  HHHHHHHHHHHHHH
  }},
  "computeProfile": {
  "roles": [
  {
  "name": "headnode",
  "targetInstanceCount": 2,
  "hardwareProfile": {
  "vmsize": "Large"
  },
  "osProfile": {
  "linuxOperatingSystemProfile": {
  "username": "UUUUUUUUU",
  "password": "PPPPPPPPP"
  }}},
  {
  "name": "workernode",
  "targetInstanceCount": WWWWWW,
  "hardwareProfile": {
  "vmsize": "Large"
  },
  "osProfile": {
  "linuxOperatingSystemProfile": {
  "username": "UUUUUUUUU",
  "password": "PPPPPPPPP"
  }}},
  {
  "name": "zookeepernode",
  "targetInstanceCount": 3,
  "hardwareProfile": {
  "vmsize": "Small"
  },
  "osProfile": {
  "linuxOperatingSystemProfile": {
  "username": "UUUUUUUUU",
  "password": "PPPPPPPPP"
  }}}



  ]}}}'

  HIVEJSON = '
  ",hive-site": {
    "javax.jdo.option.ConnectionDrivername": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "javax.jdo.option.ConnectionURL": "jdbc:sqlserver://SSSSSSSSSS;database=DDDDDDDDDD;encrypt=true;trustServerCertificate=true;create=false;loginTimeout=300",
    "javax.jdo.option.ConnectionUsername": "UUUUUUUUU",
    "javax.jdo.option.ConnectionPassword": "PPPPPPPPP"
  },
  "hive-env": {
    "hive_database": "Existing MSSQL Server database with SQL authentication",
    "hive_database_name": "HIVEDB",
    "hive_database_type": "mssql",
    "hive_existing_mssql_server_database": "HIVEDB",
    "hive_existing_mssql_server_host": "SSSSSSSSSS",
    "hive_hostname": "SSSSSSSSSS"
  }'

  bodyI <- gsub("CCCCCC", clustername, bodyI)
  bodyI <- gsub("UUUUUUUUU", sshUser, bodyI)
  bodyI <- gsub("PPPPPPPPP", sshPassword, bodyI)
  bodyI <- gsub("AAAAAAAAAAA", adminUser, bodyI)
  bodyI <- gsub("QQQQQQQQQQQ", adminPassword, bodyI)
  bodyI <- gsub("WWWWWW", workers, bodyI)
  bodyI <- gsub("SSSSSSSSSSSS", SUBIDI, bodyI)
  bodyI <- gsub("LLLLLLLLLLL", location, bodyI)
  bodyI <- gsub("TTTTTTTTTTT", storageAccount, bodyI)
  bodyI <- gsub("KKKKKKKKKKKKKKKK", storageKey, bodyI)
  bodyI <- gsub("DDDDDDDDD", kind, bodyI)
  bodyI <- gsub("VVVV", version, bodyI)
  if (HIVE) {
    HIVEJSON <- gsub("SSSSSSSSSS", hiveServer, HIVEJSON)
    HIVEJSON <- gsub("DDDDDDDDDD", hiveDB, HIVEJSON)
    HIVEJSON <- gsub("UUUUUUUUU", hiveUser, HIVEJSON)
    HIVEJSON <- gsub("PPPPPPPPP", hiveServer, hivePassword)
    bodyI <- gsub("HHHHHHHHHHHHHH", HIVEJSON, bodyI)
  } else {
    bodyI <- gsub("HHHHHHHHHHHHHH", "", bodyI)
  }

  URL <- paste0("https://management.azure.com/subscriptions/", SUBIDI,
               "/resourceGroups/", RGI,
               "/providers/Microsoft.HDInsight/clusters/", clustername,
               "?api-version=2015-03-01-preview")

    r <- PUT(URL, add_headers(.headers = c(Host = "management.azure.com",
                                           Authorization = ATI,
                                           `Content-type` = "application/json")),
             body = bodyI,
             encode = "json",
           verbosity)

  if (!status_code(r) %in% c(200, 201)) stopWithAzureError(r)
  rl <- content(r, "text", encoding = "UTF-8")
  if (toupper(mode) == "SYNC") {
    azureActiveContext$resourceGroup <- RGI
    writeLines(paste("azureResizeHDI: Request Submitted: ", Sys.time()))
    writeLines("Runing(C), Succeeded(S)")
    a <- 1
    while (a > 0) {
      rc <- azureListHDI(azureActiveContext, clustername = clustername)
      rc1 <- rc[8, 1]
      if (rc1 == "Succeeded") {
        cat("S")
        writeLines("")
        writeLines(paste("Finished Creating Sucessfully: ", Sys.time()))
        (break)()
      }
      if (rc1 == "Error") {
        writeLines("")
        writeLines(paste("Error Creating: ", Sys.time()))
        (break)()
      }
      a <- a + 1
      if (rc1 == "InProgress") {
        rc1 <- "R"
      }
      cat(rc1)
      if (a > 500)
        (break)()
      Sys.sleep(5)
    }
    # RT <- clusters[12,1]
  }
  azureActiveContext$hdiAdmin <- adminUser
  azureActiveContext$hdiPassword  <- adminPassword
  azureActiveContext$clustername <- clustername
  writeLines(paste("Finished: ", Sys.time()))
  return("Done")}


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
                                 parameters = "", azToken, subscriptionID, verbose = FALSE) {
  azureCheckToken(azureActiveContext)
  if (missing(clustername)) {
    CN <- azureActiveContext$clustername
  } else (CN <- clustername)
  if (missing(resourceGroup)) {
    RGI <- azureActiveContext$resourceGroup
  } else (RGI <- resourceGroup)
  if (missing(subscriptionID)) {
    SUBIDI <- azureActiveContext$subscriptionID
  } else (SUBIDI <- subscriptionID)
  if (missing(azToken)) {
    ATI <- azureActiveContext$Token
  } else (ATI <- azToken)
  verbosity <- if (verbose)
    httr::verbose(TRUE) else NULL

  if (!length(CN)) {
    stop("Error: No Valid clustername provided")
  }
  if (!length(RGI)) {
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


  URL <- paste("https://management.azure.com/subscriptions/", SUBIDI,
               "/resourceGroups/", RGI, "/providers/Microsoft.HDInsight/clusters/",
               CN, "/executeScriptActions?api-version=2015-03-01-preview", sep = "")

  r <- POST(URL, add_headers(.headers = c(Host = "management.azure.com",
                                          Authorization = ATI,
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

  azureActiveContext$clustername <- CN
  return("Accepted")
}


#' Get all HDInsight Script Action History for a specified clustername.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureListHDI
#' @inheritParams azureRunScriptAction
#'
#' @return Returns Dataframe of HDInsight Clusters
#' @family HDInsight functions
#' @export
azureScriptActionHistory <- function(azureActiveContext, resourceGroup,
                                     clustername = "*", subscriptionID, azToken,
                                     name, type, verbose = FALSE) {
  azureCheckToken(azureActiveContext)
  if (missing(azToken)) {
    AT <- azureActiveContext$Token
  } else (AT <- azToken)
  if (missing(clustername)) {
    CN <- azureActiveContext$clustername
  } else (CN <- clustername)
  if (missing(subscriptionID)) {
    SUBIDI <- azureActiveContext$subscriptionID
  } else (SUBIDI <- subscriptionID)
  if (missing(resourceGroup)) {
    RGI <- azureActiveContext$resourceGroup
  } else (RGI <- resourceGroup)
  verbosity <- if (verbose)
    httr::verbose(TRUE) else NULL

  if (!length(AT)) {
    stop("Error: No Token / Not currently Authenticated.")
  }
  if (!length(SUBIDI)) {
    stop("Error: No subscriptionID provided: Use SUBID argument or set in AzureContext")
  }
  if (clustername != "*" && !length(RGI)) {
    stop("Error: No resourceGroup Defined.")
  }

  # https://management.azure.com/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupname}/providers/Microsoft.HDInsight/clusters/{clustername}/scriptExecutionHistory/{scriptExecutionId}?api-version={api-version}

  URL <- paste("https://management.azure.com/subscriptions/", SUBIDI,
               "/resourceGroups/", RGI, "/providers/Microsoft.HDInsight/clusters/",
               CN, "/scriptExecutionHistory/?api-version=2015-03-01-preview",
               sep = "")

  r <- GET(URL, add_headers(.headers = c(Host = "management.azure.com",
                                         Authorization = AT,
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
  azureActiveContext$clustername <- CN

  return(dfn)
}

