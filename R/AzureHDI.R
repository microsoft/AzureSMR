#' Get all HDInsight Clusters in default Subscription or details for a specified ClusterName.
#'
#' @inheritParams SetAzureContext
#' @param ClusterName ResourceGroup Object (or use AzureActiveContext)
# @param Token Token Object (or use AzureActiveContext)
#' @param verbose Print Tracing information (Default False)
#'
#' @family HDInsight
#'
#' @return Returns Dataframe of HDInsight Clusters
#' @export
AzureListHDI <- function(AzureActiveContext,ResourceGroup,ClusterName="*",
                         SubscriptionID,ATI,Name, Type,Location,verbose = FALSE) {

  AzureCheckToken(AzureActiveContext)
  if(missing(ATI)) {AT <- AzureActiveContext$Token} else (AT = ATI)
  if(missing(SubscriptionID)) {SUBIDI <- AzureActiveContext$SubscriptionID} else (SUBIDI = SubscriptionID)
  if(missing(ResourceGroup)) {RGI <- AzureActiveContext$ResourceGroup} else (RGI = ResourceGroup)
  verbosity <- if(verbose) httr::verbose(TRUE) else NULL

  if (!length(AT)) {stop("Error: No Token / Not currently Authenticated.")}
  if (!length(SUBIDI)) {stop("Error: No SubscriptionID provided: Use SUBID argument or set in AzureContext")}
  if (ClusterName != "*" && !length(RGI)) {stop("Error: No ResourceGroup Defined.")}

  if (ClusterName == "*")
    URL <- paste("https://management.azure.com/subscriptions/",SUBIDI,"/providers/Microsoft.HDInsight/clusters?api-version=2015-03-01-preview",sep="")
  else
    URL <- paste("https://management.azure.com/subscriptions/",SUBIDI,"/resourceGroups/",RGI,"/providers/Microsoft.HDInsight/clusters/",ClusterName,"?api-version=2015-03-01-preview",sep="")

  #  print(URL)

  r <- GET(URL,add_headers(.headers = c("Host" = "management.azure.com" ,"Authorization" = AT, "Content-Type" = "application/json")),verbosity)
  rl <- content(r,"text")
  #print(rl)
  df <- fromJSON(rl)
  #print(df)
  #  dfn <- as.data.frame(paste(mydf$value$name,df$value$name))
  #dfn <- as.data.frame("")
  if (ClusterName == "*")
  {
    dfn <- as.data.frame(df$value$name)
    clust <- nrow(dfn)
    if (clust < 1) {warning("No HDInsight Clusters found.");
      return(NULL)
    }
    dfn[1:clust,1] <- df$value$name
    dfn[1:clust,2] <- df$value$id
    dfn[1:clust,3] <- df$value$location
    dfn[1:clust,4] <- df$value$type
    dfn[1:clust,5] <- df$value$properties$tier
    dfn[1:clust,6] <- df$value$properties$clusterDefinition$kind
    dfn[1:clust,7] <- df$value$properties$osType
    dfn[1:clust,8] <- df$value$properties$provisioningState
    dfn[1:clust,9] <- df$value$properties$clusterState
    dfn[1:clust,10] <- df$value$properties$createdDate
    dfn[1:clust,11] <- df$value$properties$quotaInfo$coresUsed

    roles1 <- df$value$properties$computeProfile$roles
    j=1
    for (roles in roles1)
    {
      rt=""
      for(i in 1:nrow(roles)) {
        row <- roles[i,]
        rt <- paste(rt,row$name,"(",row$targetInstanceCount,"*",row[,3],")")
      }
      dfn[j,12] <- rt
      j =j + 1
    }
  }
  else
  {

    dfn <- as.data.frame(df$name)
    clust <- nrow(dfn)
    if (clust < 1) {warning("No HDInsight Clusters found");return(NULL)}

    dfn[1,1] <- df$name
    dfn[1,2] <- df$id
    dfn[1,3] <- df$location
    dfn[1,4] <- df$type
    dfn[1,5] <- df$properties$tier
    dfn[1,6] <- df$properties$clusterDefinition$kind
    dfn[1,7] <- df$properties$osType
    dfn[1,8] <- df$properties$provisioningState
    dfn[1,9] <- df$properties$clusterState
    dfn[1,10] <- df$properties$createdDate
    dfn[1,11] <- df$properties$quotaInfo$coresUsed
    roles1 <- df$properties$computeProfile$roles
    rt=""
    for(i in 1:nrow(roles1)) {
      row <- roles1[i,]
      rt <- paste(rt,row$name,"(",row$targetInstanceCount,"*",row[,3],")")
    }
    dfn[1,12] <- rt
  }
  colnames(dfn) <- c("Name", "ID", "Location", "Type", "Tier","Kind","OS","ProvState","Status","Created","NumCores","Information")
  AzureActiveContext$ResourceGroup <- RGI

  return(t(dfn))
}


#' Get Configuration Information for a specified ClusterName.
#'
#' @inheritParams SetAzureContext
#' @inheritParams AzureListHDI
#'
#' @family HDInsight
#'
#' @return Returns Dataframe of HDInsight Clusters information
#' @export
AzureHDIConf <- function(AzureActiveContext,ClusterName,ResourceGroup,SUBID,ATI,Name, Type,Location,verbose = FALSE) {
  AzureCheckToken(AzureActiveContext)
  if(missing(ATI)) {AT <- AzureActiveContext$Token} else (AT = ATI)
  if(missing(SUBID)) {SUBIDI <- AzureActiveContext$SubscriptionID} else (SUBIDI = SUBID)
  if(missing(ResourceGroup)) {RGI <- AzureActiveContext$ResourceGroup} else (RGI = ResourceGroup)
  if(missing(ClusterName)) {CN <- AzureActiveContext$ClusterName} else (CN = ClusterName)
  verbosity <- if(verbose) httr::verbose(TRUE) else NULL

  if (!length(AT)) {stop("Error: No Token / Not currently Authenticated.")}
  if (!length(SUBIDI)) {stop("Error: No SubscriptionID provided: Use SUBID argument or set in AzureContext")}
  if (!length(CN)) {stop("Error: No ClusterName Provided.")}

  URL <- paste("https://management.azure.com/subscriptions/",SUBIDI,"/resourceGroups/",RGI,"/providers/Microsoft.HDInsight/clusters/",ClusterName,"?api-version=2015-03-01-preview",sep="")

  r <- GET(URL,add_headers(.headers = c("Host" = "management.azure.com" ,"Authorization" = AT, "Content-Type" = "application/json")),verbosity)
  rl <- content(r,"text")
  # print(rl)
  df <- fromJSON(rl)
  dfn <- as.data.frame(df$name)
  clust <- nrow(dfn)
  if (clust < 1) {warning("No HDInsight Clusters found");return(NULL)}

  dfn[1,1] <- df$name
  dfn[1,2] <- df$id
  dfn[1,3] <- df$location
  dfn[1,4] <- df$type
  dfn[1,5] <- df$properties$tier
  dfn[1,6] <- df$properties$clusterDefinition$kind
  dfn[1,7] <- df$properties$osType
  dfn[1,8] <- df$properties$provisioningState
  dfn[1,9] <- df$properties$clusterState
  dfn[1,10] <- df$properties$createdDate
  dfn[1,11] <- df$properties$quotaInfo$coresUsed
  roles1 <- df$properties$computeProfile$roles
  rt=""
  for(i in 1:nrow(roles1)) {
    row <- roles1[i,]
    rt <- paste(rt,row$name,"(",row$targetInstanceCount,"*",row[,3],")")
  }
  dfn[1,12] <- rt
  colnames(dfn) <- c("Name", "ID", "Location", "Type", "Tier","Kind","OS","ProvState","Status","Created","NumCores","Information")

  return(t(dfn))
}


#' Resize a HDInsight CLuster Role.
#'
#' @inheritParams SetAzureContext
#' @inheritParams AzureListHDI
#'
#' @family HDInsight
# @param AzureActiveContext Azure Context Object
# @param ClusterName ResourceGroup Object (or use AzureActiveContext)
# @param Role Role Type (Worker, Head, Edge)
# @param Size Desired size of Role Type
# @param MODE MODE Sync/Async
# @param Token Token Object (or use AzureActiveContext)
# @param SubscriptionID SubscriptionID Object (or use AzureActiveContext)
# @param ResourceGroup ResourceGroup Object (or use AzureActiveContext)
# @param verbose Print Tracing information (Default False)
#'
#' @export
AzureResizeHDI <- function(AzureActiveContext,ClusterName, Role="worker", Size=2, Mode="Sync",AzToken, SubscriptionID,ResourceGroup,verbose = FALSE) {
  AzureCheckToken(AzureActiveContext)

  if(missing(ResourceGroup)) {RGI <- AzureActiveContext$ResourceGroup} else (RGI = ResourceGroup)
  if(missing(SubscriptionID)) {SUBIDI <- AzureActiveContext$SubscriptionID} else (SUBIDI = SubscriptionID)
  if(missing(AzToken)) {AT <- AzureActiveContext$Token} else (AT = AzToken)
  verbosity <- if(verbose) httr::verbose(TRUE) else NULL

  if (!length(RGI)) {stop("Error: No ResourceGroup provided: Use ResourceGroup argument or set in AzureContext")}
  if (!length(ClusterName)) {stop("Error: No ClusterName provided")}
  if (!length(Role)) {stop("Error: No Role Provided")}
  if (!length(RGI)) {stop("Error: No New role Size provided")}
  verbosity <- if(verbose) httr::verbose(TRUE) else NULL

  URL <- paste("https://management.azure.com/subscriptions/",SUBIDI,"/resourceGroups/",RGI,"/providers/Microsoft.HDInsight/clusters/",ClusterName,"/roles/",Role,"/resize?api-version=2015-03-01-preview",sep="")
  #  URL <- paste("https://management.azure.com/subscriptions/",SUBIDI,"/resourceGroups/",RGI,"/providers/Microsoft.HDInsight/clusters/",ClusterName,"/resize?api-version=2015-03-01-preview",sep="")

  bodyI <- list(targetInstanceCount = Size)
  #bodyI = paste("{\"targetInstanceCount\":",Size,"}")
  #  print(URL)
  #  print(bodyI)

  r <- POST(URL,add_headers(.headers = c("Host" = "management.azure.com" ,"Authorization" = AT, "Content-Type" = "application/json")),body = bodyI, encode = "json",verbosity)

  rl <- content(r,"text",encoding="UTF-8")
  if (status_code(r) != 202) {stop(paste("Error: Return code",status_code(r) ))}
  RT <- "Request accepted"
  if(toupper(Mode) == "SYNC")
  {
    AzureActiveContext$ResourceGroup <- RGI
    writeLines(paste("AzureResizeHDI: Request Submitted: ",Sys.time()))
    writeLines("Accepted(A), Resizing(R), Succeeded(S)")
    a=1
    while (a>0)
    {
      rc <- AzureListHDI(AzureActiveContext,ClusterName=ClusterName)
      rc1 <- rc[9,1]
#            cat(paste(rc," "))
      if (rc1 == "Running"){
              cat("R")
              writeLines("")
              writeLines(paste("Finished Resizing Sucessfully: ",Sys.time()))
              break()
      }

      if (rc1 == "Error"){
        writeLines("")
        writeLines(paste("Error Resizing: ",Sys.time()))
        break()
      }

      a=a+1
      if (rc1 == "Accepted") {rc1<-"A"}
      if (rc1 == "InProgress") {rc1<-"R"}
      if (rc1 == "AzureVMConfiguration") {rc1<-"R"}
      if (rc1 == "HdInsightConfiguration") {rc1<-"R"}
      cat(rc1)

      if( a > 500) break()
      Sys.sleep(5)
    }
#    RT <- clusters[12,1]
  }
  writeLines(paste("Finished: ",Sys.time()))

  return("Done")
}


#' Delete Specifed HDInsight Cluster.
#'
#' @inheritParams SetAzureContext
#' @inheritParams AzureListHDI
#'
#' @family HDInsight
#'
#' @return Returns Dataframe of HDInsight Clusters information
#' @export
AzureDeleteHDI <- function(AzureActiveContext,ClusterName,AzToken, SubscriptionID,ResourceGroup,verbose = FALSE) {

  AzureCheckToken(AzureActiveContext)
  if(missing(ClusterName)) {CN <- AzureActiveContext$ClusterName} else (CN = ClusterName)
  if(missing(ResourceGroup)) {RGI <- AzureActiveContext$ResourceGroup} else (RGI = ResourceGroup)
  if(missing(SubscriptionID)) {SUBIDI <- AzureActiveContext$SubscriptionID} else (SUBIDI = SubscriptionID)
  if(missing(AzToken)) {ATI <- AzureActiveContext$Token} else (ATI = AzToken)
  verbosity <- if(verbose) httr::verbose(TRUE) else NULL

  if (!length(CN)) {stop("Error: No Valid ClusterName provided")}
  if (!length(RGI)) {stop("Error: No ResourceGroup provided: Use ResourceGroup argument or set in AzureContext")}

  URL <- paste("https://management.azure.com/subscriptions/",SUBIDI,"/resourceGroups/",RGI,"/providers/Microsoft.HDInsight/clusters/",CN,"?api-version=2015-03-01-preview",sep="")

  print(URL)

  r <- DELETE(URL,add_headers(.headers = c("Host" = "management.azure.com" ,"Authorization" = ATI, "Content-Type" = "application/json")),verbosity)
  if (status_code(r) != 202) {stop(paste("Error: Return code",status_code(r) ))}

  rl <- content(r,"text",encoding="UTF-8")
  #print(rl)

  return("Delete Request Accepted")
}


#' Create Specifed HDInsight Cluster.
#'
#' @inheritParams SetAzureContext
#' @inheritParams AzureListHDI
#'
#' @family HDInsight
# @param AzureActiveContext - Azure Context Object
# @param ClusterName - ResourceGroup Object (or use AzureActiveContext)
#' @param Location - Location String
#' @param Kind - Kind (spark/hadoop) DEFAULT{spark}
# @param StorageAcc - Storage Account Name
# @param SKey - Storage Key
#' @param Workers - # of Workers (Default 2)
# @param ResourceGroup - ResourceGroup Object (or use AzureActiveContext)
# @param AzToken - Token Object (or use AzureActiveContext)
# @param SubscriptionID - SubscriptionID Object (or use AzureActiveContext)
# @param verbose - Print Tracing information (Default False)
#'
#' @return Returns Success Message
#' @export
AzureCreateHDI <- function(AzureActiveContext,ClusterName,
                           Location,Kind = "spark",
                           StorageAcc, SKey, Version="3.4", Workers=2,
                           AdminUser,AdminPassword,
                           SSHUser,SSHPassword,
                           HiveServer,HiveDB,HiveUser,HivePassword,
                           ResourceGroup,AzToken, SubscriptionID,Mode="Sync",verbose = FALSE) {
  AzureCheckToken(AzureActiveContext)

  if(missing(ResourceGroup)) {RGI <- AzureActiveContext$ResourceGroup} else (RGI = ResourceGroup)
  if(missing(SubscriptionID)) {SUBIDI <- AzureActiveContext$SubscriptionID} else (SUBIDI = SubscriptionID)
  if(missing(AzToken)) {ATI <- AzureActiveContext$Token} else (ATI = AzToken)
  verbosity <- if(verbose) httr::verbose(TRUE) else NULL

  if (!length(StorageAcc)) {stop("Error: No Storage Account(StorageAcc) provided")}
#  if (!length(SKey)) {stop("Error: No Storage Key (SKey) provided")}
  if (!length(Location)) {stop("Error: No Location provided")}
  if (!length(ClusterName)) {stop("Error: No Valid ClusterName provided")}
  if (!length(SSHUser)) {stop("Error: No Valid SSHUser provided")}
  if (!length(SSHPassword)) {stop("Error: No Valid SSHPassword provided")}
  if (!length(AdminUser)) {stop("Error: No Valid AdminUser provided")}
  if (!length(AdminPassword)) {stop("Error: No Valid AdminPassword provided")}
  if (!length(RGI)) {stop("Error: No ResourceGroup provided: Use ResourceGroup argument or set in AzureContext")}

  cat("Fetching Storage Key..")
  SKey <- AzureSAGetKey(AzureActiveContext,ResourceGroup = RGI,StorageAccount = StorageAcc)

  HIVE <- FALSE
  print("1")
  if (!missing(HiveServer)) {
    print("2")

    HIVE <- TRUE
    if (!length(HiveDB)) {stop("Error: HiveServer: No Valid HiveDB provided")}
    if (!length(HiveUser)) {stop("Error: HiveServer: No Valid HiveUser provided")}
    if (!length(HivePassword)) {stop("Error: HiveServer: No Valid HivePassword provided")}
  }

  if((!grepl("[A-Z]", AdminPassword))) print("The Admin Password must be greater than 6 characters and contain at least one uppercase char, one lowercase char and one digit")
  if((!grepl("[a-z]", AdminPassword))) print("The Admin Password must be greater than 6 characters and contain at least one uppercase char, one lowercase char and one digit")
  if((!grepl("[0-9]", AdminPassword))) print("The Admin Password must be greater than 6 characters and contain at least one uppercase char, one lowercase char and one digit")
  if((!grepl("[A-Z]", SSHPassword))) print("The SSH Password must be greater than 6 characters and contain at least one uppercase char, one lowercase char and one digit")
  if((!grepl("[a-z]", SSHPassword))) print("The SSH Password must be greater than 6 characters and contain at least one uppercase char, one lowercase char and one digit")
  if((!grepl("[0-9]", SSHPassword))) print("The SSH Password must be greater than 6 characters and contain at least one uppercase char, one lowercase char and one digit")

  SKEY ="oNgZByVwfm2V1CslTHe28e4LJXeY+JjP2f9RLIOg89g2q696EgTRhJkQpHCkOx1Wg0JvowraTgHxT1uKEHM2hA=="

  bodyI = '
  {
    "id":"/subscriptions/SSSSSSSSSSSS/resourceGroups/Analytics/providers/Microsoft.HDInsight/clusters/CCCCCC",
  "name":"CCCCCC",
  "type":"Microsoft.HDInsight/clusters",
  "location": "LLLLLLLLLLL",
  "tags": { "tag1": "value1", "tag2": "value2" },
  "properties": {
  "clusterVersion": "VVVV",
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
  "vmSize": "Large"
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
  "vmSize": "Large"
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
  "vmSize": "Small"
  },
  "osProfile": {
  "linuxOperatingSystemProfile": {
  "username": "UUUUUUUUU",
  "password": "PPPPPPPPP"
  }}}



  ]}}}'

  HIVEJSON = '
  ",hive-site": {
    "javax.jdo.option.ConnectionDriverName": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "javax.jdo.option.ConnectionURL": "jdbc:sqlserver://SSSSSSSSSS;database=DDDDDDDDDD;encrypt=true;trustServerCertificate=true;create=false;loginTimeout=300",
    "javax.jdo.option.ConnectionUserName": "UUUUUUUUU",
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

  bodyI <- gsub("CCCCCC",ClusterName,bodyI)
  bodyI <- gsub("UUUUUUUUU",SSHUser,bodyI)
  bodyI <- gsub("PPPPPPPPP",SSHPassword,bodyI)
  bodyI <- gsub("AAAAAAAAAAA",AdminUser,bodyI)
  bodyI <- gsub("QQQQQQQQQQQ",AdminPassword,bodyI)
  bodyI <- gsub("WWWWWW",Workers,bodyI)
  bodyI <- gsub("SSSSSSSSSSSS",SUBIDI,bodyI)
  bodyI <- gsub("LLLLLLLLLLL",Location,bodyI)
  bodyI <- gsub("TTTTTTTTTTT",StorageAcc,bodyI)
  bodyI <- gsub("KKKKKKKKKKKKKKKK",SKey,bodyI)
  bodyI <- gsub("DDDDDDDDD",Kind,bodyI)
  bodyI <- gsub("VVVV",Version,bodyI)
  if (HIVE)
  {
    print("HERE")
    HIVEJSON <- gsub("SSSSSSSSSS",HiveServer,HIVEJSON)
    HIVEJSON <- gsub("DDDDDDDDDD",HiveDB,HIVEJSON)
    HIVEJSON <- gsub("UUUUUUUUU",HiveUser,HIVEJSON)
    HIVEJSON <- gsub("PPPPPPPPP",HiveServer,HivePassword)
    bodyI <- gsub("HHHHHHHHHHHHHH",HIVEJSON,bodyI)
  }
  else
    bodyI <- gsub("HHHHHHHHHHHHHH","",bodyI)

#  print(bodyI)
#  return(bodyI)
    URL <- paste("https://management.azure.com/subscriptions/",SUBIDI,"/resourceGroups/",RGI,"/providers/Microsoft.HDInsight/clusters/",ClusterName,"?api-version=2015-03-01-preview",sep="")

#  URL <- paste("https://management.azure.com/subscriptions/",SUBIDI,"/providers/Microsoft.HDInsight/locations/","northeurope/validateCreateRequest","?api-version=2015-03-01-preview",sep="")

#  PUT   https://management.azure.com/subscriptions/{subscriptionId}/providers/Microsoft.HDInsight/locations/{location}/validateCreateRequest?api-version={api-version}

  print(URL)
  #r <- PUT(URL,add_headers(.headers = c("Host" = "management.azure.com" ,"Authorization" = ATI, "Content-Type" = "application/json")),body = bodyI, encode = "json",verbosity)

  r <- PUT(URL,add_headers(.headers = c("Host" = "management.azure.com" ,"Authorization" = ATI, "Content-Type" = "application/json")),body = bodyI, encode = "json",verbosity)


  rl <- content(r,"text",encoding="UTF-8")
  if(toupper(Mode) == "SYNC")
  {
    AzureActiveContext$ResourceGroup <- RGI
    writeLines(paste("AzureResizeHDI: Request Submitted: ",Sys.time()))
    writeLines("Runing(C), Succeeded(S)")
    a=1
    while (a>0)
    {
      rc <- AzureListHDI(AzureActiveContext,ClusterName=ClusterName)
      rc1 <- rc[8,1]
      #      cat(paste(rc," "))
      if (rc1 == "Succeeded"){
        cat("S")
        writeLines("")
        writeLines(paste("Finished Creating Sucessfully: ",Sys.time()))
        break()
      }
      if (rc1 == "Error"){
        writeLines("")
        writeLines(paste("Error Creating: ",Sys.time()))
        break()
      }

      a=a+1
      if (rc1 == "InProgress") {rc1<-"R"}
      cat(rc1)

      if( a > 500) break()
      Sys.sleep(5)
    }
#    RT <- clusters[12,1]
  }
  AzureActiveContext$HDIAdmin <- AdminUser
  AzureActiveContext$HDIPassword <- AdminPassword
  AzureActiveContext$ClusterName <- ClusterName

  writeLines(paste("Finished: ",Sys.time()))
  return("Done")
}


#' Run Script Action on HDI Cluster.
#'
#' @inheritParams SetAzureContext
#' @inheritParams AzureListHDI
#' @inheritParams AzureListVM
#'
#' @param ScriptName - Identifier for Custom action scrript operation
#' @param ScriptURL - URL to custom action script (Sring)
# @param Roles - Specificy the Roles to apply action string (workernode,headnode,edgenode)
#' @param HeadNode - install on head nodes (default FALSE)
#' @param WorkerNode - install on worker nodes (default FALSE)
#' @param EdgeNode - install on worker nodes (default FALSE)
#'
#' @family HDInsight
#' @return Returns Success Message
#' @export
AzureRunScriptAction <- function(AzureActiveContext,ScriptName = "script1",ScriptURL,
                                 HeadNode=TRUE,WorkerNode=FALSE,EdgeNode=FALSE,
                                 ClusterName,ResourceGroup,Parameters="",
                                 AzToken, SubscriptionID,verbose = FALSE) {
  AzureCheckToken(AzureActiveContext)
  if(missing(ClusterName)) {CN <- AzureActiveContext$ClusterName} else (CN = ClusterName)
  if(missing(ResourceGroup)) {RGI <- AzureActiveContext$ResourceGroup} else (RGI = ResourceGroup)
  if(missing(SubscriptionID)) {SUBIDI <- AzureActiveContext$SubscriptionID} else (SUBIDI = SubscriptionID)
  if(missing(AzToken)) {ATI <- AzureActiveContext$Token} else (ATI = AzToken)
  verbosity <- if(verbose) httr::verbose(TRUE) else NULL

  if (!length(CN)) {stop("Error: No Valid ClusterName provided")}
  if (!length(RGI)) {stop("Error: No ResourceGroup provided: Use ResourceGroup argument or set in AzureContext")}
  if (!length(ScriptName)) {stop("Error: No Valid ScriptName provided")}
  if (!length(ScriptURL)) {stop("Error: No Valid ScriptURL provided")}

  RL <- ""
  if (HeadNode == TRUE)
    RL <= '"headnode"'

  if (HeadNode == TRUE) RL <- '"headnode"'

  if (WorkerNode == TRUE)
    if (nchar(RL) == 0) RL <- '"workernode"' else RL <- paste0(RL,',"workernode"')
  if (EdgeNode == TRUE)
    if (nchar(RL) == 0) RL <- '"edgenode"' else RL <- paste0(RL,',"edgenode"')

  if (nchar(RL) == 0)
    stop("Error: No Role(HeadNode,WorkerNode,EdgeNode) flag set to TRUE")
  bodyI = '
{
  "scriptActions": [
  {
  "name": "NNNNNNNNNN",
  "uri": "UUUUUUUUUUU",
  "parameters": "PPPPPPPPPPPP",
  "roles": [RRRRRRRRRRRRRR]
  }
  ],
  "persistOnSuccess": true
}'
  bodyI <- gsub("NNNNNNNNNN",ScriptName,bodyI)
  bodyI <- gsub("UUUUUUUUUUU",ScriptURL,bodyI)
  bodyI <- gsub("PPPPPPPPPPPP",Parameters,bodyI)
  bodyI <- gsub("RRRRRRRRRRRRRR",RL,bodyI)

  #https://management.azure.com/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.HDInsight/clusters/{clustername}/executeScriptActions?api-version={api-version}

#  print(bodyI)

  URL <- paste("https://management.azure.com/subscriptions/",SUBIDI,"/resourceGroups/",RGI,"/providers/Microsoft.HDInsight/clusters/",CN,"/executeScriptActions?api-version=2015-03-01-preview",sep="")

  r <- POST(URL,add_headers(.headers = c("Host" = "management.azure.com" ,"Authorization" = ATI, "Content-Type" = "application/json")),body = bodyI, encode = "json",verbosity)

  rl <- content(r,"text",encoding="UTF-8")

  if (status_code(r) == 409) {stop(paste("Error: Conflict(Action script in progress on cluster?)",status_code(r) ))}

  if (status_code(r) != 202) {stop(paste("Error: Return code",status_code(r) ))}

  AzureActiveContext$ClusterName <- CN
  return("Accepted")
}


#' Get all HDInsight Script Action Historyfor a specified ClusterName.
#'
#' @inheritParams SetAzureContext
#' @inheritParams AzureListHDI
#' @inheritParams AzureRunScriptAction
#'
#' @family HDInsight
#'
#' @return Returns Dataframe of HDInsight Clusters
#' @export
AzureScriptActionHistory <- function(AzureActiveContext,ResourceGroup,
                                     ClusterName="*",SubscriptionID,ATI,Name, Type,verbose = FALSE) {
    AzureCheckToken(AzureActiveContext)
    if(missing(ATI)) {AT <- AzureActiveContext$Token} else (AT = ATI)
    if(missing(ClusterName)) {CN <- AzureActiveContext$ClusterName} else (CN = ClusterName)
    if(missing(SubscriptionID)) {SUBIDI <- AzureActiveContext$SubscriptionID} else (SUBIDI = SubscriptionID)
    if(missing(ResourceGroup)) {RGI <- AzureActiveContext$ResourceGroup} else (RGI = ResourceGroup)
    verbosity <- if(verbose) httr::verbose(TRUE) else NULL

    if (!length(AT)) {stop("Error: No Token / Not currently Authenticated.")}
    if (!length(SUBIDI)) {stop("Error: No SubscriptionID provided: Use SUBID argument or set in AzureContext")}
    if (ClusterName != "*" && !length(RGI)) {stop("Error: No ResourceGroup Defined.")}

    #https://management.azure.com/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.HDInsight/clusters/{clustername}/scriptExecutionHistory/{scriptExecutionId}?api-version={api-version}

    URL <- paste("https://management.azure.com/subscriptions/",SUBIDI,"/resourceGroups/",RGI,"/providers/Microsoft.HDInsight/clusters/",CN,"/scriptExecutionHistory/?api-version=2015-03-01-preview",sep="")

    #  print(URL)

    r <- GET(URL,add_headers(.headers = c("Host" = "management.azure.com" ,"Authorization" = AT, "Content-Type" = "application/json")),verbosity)
    rl <- content(r,"text")
    #print(rl)
    df <- fromJSON(rl)
    dfn <- as.data.frame(df$value$scriptExecutionId)
    Hist <- nrow(dfn)

            if (Hist < 1) {warning("No ScriptAction history found");return(NULL)}

      dfn[1:Hist,1] <- df$value$name
      dfn[1:Hist,2] <- df$value$scriptExecutionId
      dfn[1:Hist,3] <- paste0(df$value$roles)
      dfn[1:Hist,4] <- df$value$startTime
      dfn[1:Hist,5] <- df$value$endTime

      dfn[1:Hist,6] <- df$value$status
      dfn[1:Hist,7] <- df$value$uri
      dfn[1:Hist,8] <- df$value$parameters

    colnames(dfn) <- c("Name", "ID", "Roles", "startTime","endTime","status","uri","parameters")
    AzureActiveContext$ClusterName <- CN

    return(dfn)
}

