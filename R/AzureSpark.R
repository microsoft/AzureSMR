#' Create new Spark Session.
#'
#' @inheritParams SetAzureContext
#'
#' @param ClusterName ClusterName
#' @param HDIAdmin HDIAdmin - HDinsight Administrator Name
#' @param HDIPassword HDinsight Administrator Name
#' @param Kind Kind Spark/PySpark/Spark
#' @param verbose Print Tracing information (Default False)
#'
#' @family Spark
#' @export
AzureSparkNewSession <- function(AzureActiveContext,ClusterName,HDIAdmin,HDIPassword,Kind = "spark",verbose = FALSE) {

  if(missing(ClusterName)) {CN <- AzureActiveContext$ClusterName} else (CN = ClusterName)
  if(missing(HDIAdmin)) {HA <- AzureActiveContext$HDIAdmin} else (HA = HDIAdmin)
  if(missing(HDIPassword)) {HP <- AzureActiveContext$HDIPassword} else (HP = HDIPassword)
  if(missing(Kind)) {KI <- AzureActiveContext$Kind} else (KI = Kind)
  verbosity <- if(verbose) httr::verbose(TRUE) else NULL

  if (!length(CN)) {stop("Error: No Valid ClusterName provided")}
  if (!length(HA)) {stop("Error: No Valid HDIAdmin provided")}
  if (!length(HP)) {stop("Error: No Valid HDIPassword provided")}
  if (!length(Kind)) {stop("Error: No Valid Kind provided")}

  AzureActiveContext$HDIAdmin <- HA
  AzureActiveContext$HDIPassword <- HP
  AzureActiveContext$ClusterName <- CN
  AzureActiveContext$Kind <- KI

  URL <- paste("https://",CN,".azurehdinsight.net/livy/sessions",sep="")

  #  URL <- paste("https://management.azure.com/subscriptions/",SUBIDI,"/resourceGroups/",RGI,"/providers/Microsoft.HDInsight/clusters/",ClusterName,"?api-version=2015-03-01-preview",sep="")

  #  print(URL)

  bodyI <- list(kind = KI)

  r <- POST(URL,add_headers(.headers = c("Content-Type" = "application/json")),authenticate(HA,HP),body = bodyI, encode = "json",verbosity)

  if (status_code(r) != "201")
    stop(paste("Error Return Code:",status_code(r)))

  rl <- content(r,"text",encoding="UTF-8")
  #print(rl)
  df <- fromJSON(rl)
  #print(df$id)
  AzureActiveContext$SessionID <- toString(df$id)
  return(df$id)
}


#' List Spark Sessions.
#'
#' @inheritParams SetAzureContext
#' @inheritParams AzureSparkNewSession
#'
#' @family Spark
#' @export
AzureSparkListSessions <- function(AzureActiveContext,ClusterName,HDIAdmin,HDIPassword,verbose = FALSE) {
  HA = ""
  if(missing(ClusterName)) {CN <- AzureActiveContext$ClusterName} else (CN = ClusterName)
  if(missing(HDIAdmin)) {HA <- AzureActiveContext$HDIAdmin} else (HA = HDIAdmin)
  if(missing(HDIPassword)) {HP <- AzureActiveContext$HDIPassword} else (HP = HDIPassword)
  verbosity <- if(verbose) httr::verbose(TRUE) else NULL

  if (!length(CN)) {stop("Error: No Valid ClusterName provided")}
  if (!length(HA)) {stop("Error: No Valid HDIAdmin provided")}
  if (!length(HP)) {stop("Error: No Valid HDIPassword provided")}

  AzureActiveContext$HDIAdmin <- HA
  AzureActiveContext$HDIPassword <- HP
  AzureActiveContext$ClusterName <- CN

  URL <- paste("https://",CN,".azurehdinsight.net/livy/sessions",sep="")

  #  URL <- paste("https://management.azure.com/subscriptions/",SUBIDI,"/resourceGroups/",RGI,"/providers/Microsoft.HDInsight/clusters/",ClusterName,"?api-version=2015-03-01-preview",sep="")

  #print(URL)

  r <- GET(URL,add_headers(.headers = c("Content-Type" = "application/json")),authenticate(HA,HP),verbosity)
  #,authenticate("admin", "Summer2014!")
  rl <- content(r,"text",encoding="UTF-8")
  df <- fromJSON(rl)
  #  print(df)
  #  print(df$sessions$appId)
  dfn <- as.data.frame(df$sessions$id)
  clust <- nrow(dfn)
  if (clust == 0) stop ("No Sessions available")
  dfn[1:clust,2] <- df$sessions$appId
  dfn[1:clust,3] <- df$sessions$state
  dfn[1:clust,4] <- df$sessions$proxyUser
  dfn[1:clust,5] <- df$sessions$kind
  colnames(dfn) <- c("ID", "AppID", "State","ProxyUser","kind")

  return(dfn)
}


#' Stop a Spark Sessions.
#'
#' @inheritParams SetAzureContext
#' @inheritParams AzureSparkNewSession
#'
#' @family Spark
#' @export
AzureSparkStopSession <- function(AzureActiveContext,ClusterName,HDIAdmin,HDIPassword,SessionID,verbose = FALSE) {

  AzureCheckToken(AzureActiveContext)

  if(missing(ClusterName)) {CN <- AzureActiveContext$ClusterName} else (CN = ClusterName)
  if(missing(HDIAdmin)) {HA <- AzureActiveContext$HDIAdmin} else (HA = HDIAdmin)
  if(missing(HDIPassword)) {HP <- AzureActiveContext$HDIPassword} else (HP = HDIPassword)
  if(missing(SessionID)) {SI <- AzureActiveContext$SessionID} else (SI = SessionID)
  verbosity <- if(verbose) httr::verbose(TRUE) else NULL

  if (!length(CN)) {stop("Error: No ClusterName provided")}
  if (!length(HA)) {stop("Error: No HDIAdmin provided")}
  if (!length(HP)) {stop("Error: No HDIPassword provided")}
  if (!length(SI)) {stop("Error: No SessionID provided")}

  AzureActiveContext$HDIAdmin <- HA
  AzureActiveContext$HDIPassword <- HP
  AzureActiveContext$ClusterName <- CN
  AzureActiveContext$SessionID <- SI

  URL <- paste("https://",CN,".azurehdinsight.net/livy/sessions/",SI,sep="")

  #  URL <- paste("https://management.azure.com/subscriptions/",SUBIDI,"/resourceGroups/",RGI,"/providers/Microsoft.HDInsight/clusters/",ClusterName,"?api-version=2015-03-01-preview",sep="")

  #print(URL)

  r <- DELETE(URL,add_headers(.headers = c("Content-Type" = "application/json")),authenticate(HA,HP),verbosity)

  #rl <- content(r,"text",encoding="UTF-8")
  #print(rl)
  if (status_code(r) == "404")
    stop(paste("SessionID not found (",status_code(r),")"))

  if (status_code(r) != "200")
    stop(paste("Error Return Code:",status_code(r)))
  return("Done")
}


#' Send Spark Statements/comamnds (REPL/Interactive mode).
#'
#' @inheritParams SetAzureContext
#' @inheritParams AzureSparkNewSession
#'
#' @family Spark
#' @export
AzureSparkCMD <- function(AzureActiveContext,CMD,ClusterName,HDIAdmin,HDIPassword,SessionID,verbose = FALSE) {

  if(missing(ClusterName)) {CN <- AzureActiveContext$ClusterName} else (CN = ClusterName)
  if(missing(HDIAdmin)) {HA <- AzureActiveContext$HDIAdmin} else (HA = HDIAdmin)
  if(missing(HDIPassword)) {HP <- AzureActiveContext$HDIPassword} else (HP = HDIPassword)
  if(missing(SessionID)) {SI <- AzureActiveContext$SessionID} else (SI = SessionID)
  if(missing(CMD)) {stop("Error: No CMD provided")}
  verbosity <- if(verbose) httr::verbose(TRUE) else NULL

  if (!length(CN)) {stop("Error: No Valid ClusterName provided")}
  if (!length(HA)) {stop("Error: No Valid HDIAdmin provided")}
  if (!length(HP)) {stop("Error: No Valid HDIPassword provided")}
  if (!length(SI)) {stop("Error: No SessionID provided")}

  AzureActiveContext$HDIAdmin <- HA
  AzureActiveContext$HDIPassword <- HP
  AzureActiveContext$ClusterName <- CN
  AzureActiveContext$SessionID <- SI

  URL <- paste("https://",CN,".azurehdinsight.net/livy/sessions/",SI,"/statements",sep="")

  #  URL <- paste("https://management.azure.com/subscriptions/",SUBIDI,"/resourceGroups/",RGI,"/providers/Microsoft.HDInsight/clusters/",ClusterName,"?api-version=2015-03-01-preview",sep="")

  #print(URL)
  # print(typeof(CMD))
  bodyI <- list(code = CMD)
  #print(CMD)

  r <- POST(URL,add_headers(.headers = c("Content-Type" = "application/json")),authenticate(HA,HP),body = bodyI, encode = "json",verbosity)

  rl <- content(r,"text",encoding="UTF-8")
  rh <- headers(r)

  if (status_code(r) == "404")
    stop(paste("SessionID not found (",status_code(r),")"))

  if (status_code(r) != "201")
    stop(paste("Error Return Code:",status_code(r)))

  df <- fromJSON(rl)
  #  print(df$sessions$appId)
  if (df$state == "available")
  {
    RET <- df$output$data
    return(toString(RET))
  }
  DUR <- 2
  URL <- paste("https://",CN,".azurehdinsight.net/livy/",rh$location,sep="")
  #  print(URL)
  writeLines(paste("CMD Running: ",Sys.time()))
  writeLines("Running(R), Completed(C)")

    while (df$state == "running")
  {
    Sys.sleep(DUR)
    if (DUR < 5) DUR <- DUR +1
    cat("R")
    r <- GET(URL,add_headers(.headers = c("Content-Type" = "application/json")),authenticate(HA,HP))
    rl <- content(r,"text",encoding="UTF-8")
    rh <- headers(r)
    df <- fromJSON(rl)

  }
  cat("C")
  writeLines("")
  writeLines(paste("Finished Running statement: ",Sys.time()))
  RET <- df$output$data[1]
  #rownames(RET) <- "Return Value"
  return(toString(RET))

}


#' Submit Spark Job (Batch mode).
#'
#' @inheritParams SetAzureContext
#' @inheritParams AzureSparkNewSession
#'
#' @family Spark
#' @export
AzureSparkJob <- function(AzureActiveContext,FILE,ClusterName,HDIAdmin,HDIPassword, Log="URL",verbose = FALSE) {

  if(missing(ClusterName)) {CN <- AzureActiveContext$ClusterName} else (CN = ClusterName)
  if(missing(HDIAdmin)) {HA <- AzureActiveContext$HDIAdmin} else (HA = HDIAdmin)
  if(missing(HDIPassword)) {HP <- AzureActiveContext$HDIPassword} else (HP = HDIPassword)
  if(missing(FILE)) {stop("Error: No CMD provided")}
  verbosity <- if(verbose) httr::verbose(TRUE) else NULL

  if (!length(CN)) {stop("Error: No Valid ClusterName provided")}
  if (!length(HA)) {stop("Error: No Valid HDIAdmin provided")}
  if (!length(HP)) {stop("Error: No Valid HDIPassword provided")}

  AzureActiveContext$HDIAdmin <- HA
  AzureActiveContext$HDIPassword <- HP
  AzureActiveContext$ClusterName <- CN

  URL <- paste("https://",CN,".azurehdinsight.net/livy/batches",sep="")

  #  URL <- paste("https://management.azure.com/subscriptions/",SUBIDI,"/resourceGroups/",RGI,"/providers/Microsoft.HDInsight/clusters/",ClusterName,"?api-version=2015-03-01-preview",sep="")

  #print(URL)
  # print(typeof(CMD))
  bodyI <- list(file = FILE)
  #print(CMD)

  r <- POST(URL,add_headers(.headers = c("Content-Type" = "application/json")),authenticate(HA,HP),body = bodyI, encode = "json",verbosity)

  rl <- content(r,"text",encoding="UTF-8")
  rh <- headers(r)

  if (status_code(r) == "404")
    stop(paste("SessionID not found (",status_code(r),")"))

  if (status_code(r) != "201")
    stop(paste("Error Return Code:",status_code(r)))

  df <- fromJSON(rl)
  BI <- df$id
  #  print(df$sessions$appId)
  if (df$state == "available")
    return(df$output$data)
  DUR <- 2
  BI <- df$id

  URL <- paste("https://",CN,".azurehdinsight.net/livy/batches/",BI,sep="")
  #  print(URL)
  writeLines(paste("CMD Running: ",Sys.time()))
  writeLines("Running(R), Completed(C)")
  LOGURL2 <- ""

  while (df$state == "running")
  {
    Sys.sleep(DUR)
    if (DUR < 5) DUR <- DUR +1
    cat("R")
    r <- GET(URL,add_headers(.headers = c("Content-Type" = "application/json")),authenticate(HA,HP),verbosity)
    rl <- content(r,"text",encoding="UTF-8")
    rh <- headers(r)
    df <- fromJSON(rl)
    if (length(df$appInfo$driverLogUrl))
      LOGURL2 <- df$appInfo$driverLogUrl
  }
  cat("C")
  STATE <- df$state
  writeLines("")
  writeLines(paste("Finished Running statement: ",Sys.time()))

  #BID = gsub("application_","container_",df$appId)
  #print(df$log[2])
  #HN<- strsplit(df$log[2], " ")
  print("LOGURL")
  print(LOGURL2)

  if (Log == "URL")
    AzureActiveContext$Log <- LOGURL2
  else
  {
    print("LOGURL")
    print(LOGURL2)

    r <- GET(LOGURL2,add_headers(.headers = c("Content-Type" = "application/json")),authenticate(HA,HP),verbosity)
    rl <- content(r,"text",encoding="UTF-8")
    AzureActiveContext$Log <- rl
  }
  return(STATE)
}


#' List Spark Jobs (Batch mode).
#'
#' @inheritParams SetAzureContext
#' @inheritParams AzureSparkNewSession
#'
#' @family Spark
#' @return (manually direct output to Blob fule /SQL in script)
#' @export
AzureSparkListJobs <- function(AzureActiveContext,ClusterName,HDIAdmin,HDIPassword,KIND,verbose = FALSE) {
  HA = ""
  if(missing(ClusterName)) {CN <- AzureActiveContext$ClusterName} else (CN = ClusterName)
  if(missing(HDIAdmin)) {HA <- AzureActiveContext$HDIAdmin} else (HA = HDIAdmin)
  if(missing(HDIPassword)) {HP <- AzureActiveContext$HDIPassword} else (HP = HDIPassword)
  verbosity <- if(verbose) httr::verbose(TRUE) else NULL

  if (!length(CN)) {stop("Error: No Valid ClusterName provided")}
  if (!length(HA)) {stop("Error: No Valid HDIAdmin provided")}
  if (!length(HP)) {stop("Error: No Valid HDIPassword provided")}

  AzureActiveContext$HDIAdmin <- HA
  AzureActiveContext$HDIPassword <- HP
  AzureActiveContext$ClusterName <- CN

  URL <- paste("https://",CN,".azurehdinsight.net/livy/batches",sep="")

  #  URL <- paste("https://management.azure.com/subscriptions/",SUBIDI,"/resourceGroups/",RGI,"/providers/Microsoft.HDInsight/clusters/",ClusterName,"?api-version=2015-03-01-preview",sep="")

  #print(URL)


  r <- GET(URL,add_headers(.headers = c("Content-Type" = "application/json")),authenticate(HA,HP),verbosity)
  #,authenticate("admin", "Summer2014!")
  rl <- content(r,"text",encoding="UTF-8")
  df <- fromJSON(rl)
  #print(df$sessions$id)
  print(colnames(df))
  dfn <- as.data.frame(df$sessions$id)
  clust <- nrow(dfn)
  if (clust == 0) stop ("No Sessions available")
  dfn[1:clust,2] <- df$sessions$appId
  dfn[1:clust,3] <- df$sessions$state

  colnames(dfn) <- c("ID", "AppID", "State")

  return(dfn)
}


#' Show Spark Log Output.
#'
#' @inheritParams SetAzureContext
#' @inheritParams AzureSparkNewSession
#'
#' @family Spark
#'
#' @return Show Log in Browser
#' @export
AzureSparkShowURL <- function(AzureActiveContext,URL) {
  if (!missing(URL))
    browseURL(URL)
  else
    browseURL(AzureActiveContext$Log)
  return("")
}
