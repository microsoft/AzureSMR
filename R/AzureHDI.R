#' @name AzureSM: AzureListHDI
#' @title Get all HDInsight Clusters in default Subscription or details for a specified ClusterName
#' @param AzureActiveContext Azure Context Object
#' @param ResourceGroup ResourceGroup Object (or use AzureActiveContext)
#' @param ClusterName ResourceGroup Object (or use AzureActiveContext)
#' @param Token Token Object (or use AzureActiveContext)
#' @param SubscriptionID SubscriptionID Object (or use AzureActiveContext)
#' @param verbose Print Tracing information (Default False)
#' @rdname AzureListHDI
#' @return Returns Dataframe of HDInsight Clusters
#' @export
AzureListHDI <- function(AzureActiveContext,ResourceGroup,ClusterName="*",SUBID,ATI,Name, Type,Location,verbose = FALSE) {
  if(missing(ATI)) {AT <- AzureActiveContext$Token} else (AT = ATI)
  if(missing(SUBID)) {SUBIDI <- AzureActiveContext$SubscriptionID} else (SUBIDI = SUBID)
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
  # print(rl)
  df <- fromJSON(rl)
  #print(df)
  #  dfn <- as.data.frame(paste(mydf$value$name,df$value$name))
  #dfn <- as.data.frame("")
  if (ClusterName == "*")
  {
    dfn <- as.data.frame(df$value$name)
    clust <- nrow(dfn)
    if (clust < 1) {warning("No HDInsight Clusters found");return(NULL)}
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

  return(t(dfn))
}

#' @name AzureSM: AzureHDIConf
#' @title Get Configuration Information for a specified ClusterName
#' @param AzureActiveContext Azure Context Object
#' @param ClusterName ResourceGroup Object (or use AzureActiveContext)
#' @param ResourceGroup ResourceGroup Object (or use AzureActiveContext)
#' @param Token Token Object (or use AzureActiveContext)
#' @param SubscriptionID SubscriptionID Object (or use AzureActiveContext)
#' @param verbose Print Tracing information (Default False)
#' @rdname AzureHDIConf
#' @return Returns Dataframe of HDInsight Clusters information
#' @export
AzureHDIConf <- function(AzureActiveContext,ClusterName,ResourceGroup,SUBID,ATI,Name, Type,Location,verbose = FALSE) {
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
#' @name AzureSM: AzureResizeHDI
#' @title Resize a HDInsight CLuster Role
#' @param AzureActiveContext Azure Context Object
#' @param ClusterName ResourceGroup Object (or use AzureActiveContext)
#' @param Role Role Type (Worker, Head, Edge)
#' @param Size Desired size of Role Type
#' @param MODE MODE Sync/Async
#' @param Token Token Object (or use AzureActiveContext)
#' @param SubscriptionID SubscriptionID Object (or use AzureActiveContext)
#' @param ResourceGroup ResourceGroup Object (or use AzureActiveContext)
#' @param verbose Print Tracing information (Default False)
#' @rdname AzureResizeHDI
#' @export
AzureResizeHDI <- function(AzureActiveContext,ClusterName, Role="worker", Size=2, Mode="Sync",AzToken, SubscriptionID,ResourceGroup,verbose = FALSE) {

  if(missing(ResourceGroup)) {RGI <- AzureActiveContext$ResourceGroup} else (RGI = ResourceGroup)
  if(missing(SubscriptionID)) {SUBIDI <- AzureActiveContext$SubscriptionID} else (SUBIDI = SubscriptionID)
  if(missing(AzToken)) {AT <- AzureActiveContext$Token} else (AT = AzToken)
  verbosity <- if(verbose) httr::verbose(TRUE) else NULL

  if (!length(RGI)) {stop("Error: No ResourceGroup provided: Use ResourceGroup argument or set in AzureContext")}
  if (!length(ClusterName)) {stop("Error: No ClusterName provided")}
  if (!length(ROle)) {stop("Error: No Role Provided")}
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
  if (status_code(r) != 202) {stop(paste("Error: REturn code",status_code(r) ))}
  RT <- "Request accepted"
  if(toupper(Mode) == "SYNC")
  {
    AzureActiveContext$ResourceGroup <- RGI
    writeLines(paste("AzureResizeHDI: Request Submitted: ",Sys.time()))
    writeLines("Resizing(R), Succeeded(S)")
    a=1
    while (a>0)
    {
      rc <- AzureListHDI(ClusterName=ClusterName)
      rc1 <- rc[8,1]
      #      cat(paste(rc," "))
      if (rc1 == "Succeeded"){
        cat("S")
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
      if (rc1 == "InProgress") {rc1<-"R"}
      cat(rc1)

      if( a > 500) break()
      Sys.sleep(5)
    }
    RT <- clusters[12,1]
  }
  writeLines(paste("Finished: ",Sys.time()))

  return(RT)
}

#' @name AzureSM: AzureDeleteHDI
#' @title Delete Specifed HDInsight Cluster
#' @param AzureActiveContext Azure Context Object
#' @param ClusterName ResourceGroup Object (or use AzureActiveContext)
#' @param Token Token Object (or use AzureActiveContext)
#' @param SubscriptionID SubscriptionID Object (or use AzureActiveContext)
#' @param ResourceGroup ResourceGroup Object (or use AzureActiveContext)
#' @param verbose Print Tracing information (Default False)
#' @rdname AzureDeleteHDI
#' @return Returns Dataframe of HDInsight Clusters information
#' @export
AzureDeleteHDI <- function(AzureActiveContext,ClusterName,AzToken, SubscriptionID,ResourceGroup,verbose = FALSE) {

  if(missing(ResourceGroup)) {RGI <- AzureActiveContext$ResourceGroup} else (RGI = ResourceGroup)
  if(missing(SubscriptionID)) {SUBIDI <- AzureActiveContext$SubscriptionID} else (SUBIDI = SubscriptionID)
  if(missing(AzToken)) {ATI <- AzureActiveContext$Token} else (ATI = AzToken)
  verbosity <- if(verbose) httr::verbose(TRUE) else NULL

  if (!length(ClusterName)) {stop("Error: No Valid ClusterName provided")}
  if (!length(RGI)) {stop("Error: No ResourceGroup provided: Use ResourceGroup argument or set in AzureContext")}

  URL <- paste("https://management.azure.com/subscriptions/",SUBIDI,"/resourceGroups/",RGI,"/providers/Microsoft.HDInsight/clusters/",ClusterName,"?api-version=2015-03-01-preview",sep="")

  print(URL)

  r <- DELETE(URL,add_headers(.headers = c("Host" = "management.azure.com" ,"Authorization" = ATI, "Content-Type" = "application/json")),verbosity)
  rl <- content(r,"text",encoding="UTF-8")
  print(rl)
  return("Done")
}
