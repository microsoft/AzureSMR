#' Create new Spark Session.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureAuthenticate
#' @inheritParams azureCreateHDI
#'
#' @family Spark functions
#' @export
azureSparkNewSession <- function(azureActiveContext, clustername, hdiAdmin,
                                 hdiPassword, kind = "spark", verbose = FALSE) {

  if (missing(clustername)) {
    CN <- azureActiveContext$clustername
  } else (CN = clustername)
  if (missing(hdiAdmin)) {
    HA <- azureActiveContext$hdiAdmin
  } else (HA = hdiAdmin)
  if (missing(hdiPassword)) {
    HP <- azureActiveContext$hdiPassword
  } else (HP = hdiPassword)
  if (missing(kind)) {
    KI <- azureActiveContext$kind
  } else (KI = kind)
  verbosity <- set_verbosity(verbose)


  if (!length(CN)) {
    stop("Error: No Valid clustername provided")
  }
  if (!length(HA)) {
    stop("Error: No Valid hdiAdmin provided")
  }
  if (!length(HP)) {
    stop("Error: No Valid hdiPassword  provided")
  }
  if (!length(kind)) {
    stop("Error: No Valid kind provided")
  }

  azureActiveContext$hdiAdmin <- HA
  azureActiveContext$hdiPassword <- HP
  azureActiveContext$clustername <- CN
  azureActiveContext$kind <- KI

  URL <- paste("https://", CN, ".azurehdinsight.net/livy/sessions", sep = "")

  # URL <-
  # paste('https://management.azure.com/subscriptions/',subscriptionID,'/resourceGroups/',resourceGroup,'/providers/Microsoft.HDInsight/clusters/',clustername,'?api-version=2015-03-01-preview',sep='')

  # print(URL)

  bodyI <- list(kind = KI)

  r <- POST(URL, add_headers(.headers = c(`Content-type` = "application/json")),
            authenticate(HA, HP), body = bodyI, encode = "json", verbosity)

  if (status_code(r) != "201")
    stop(paste("Error Return Code:", status_code(r)))

  rl <- content(r, "text", encoding = "UTF-8")
  # print(rl)
  df <- fromJSON(rl)
  # print(df$id)
  azureActiveContext$sessionID <- toString(df$id)
  return(df$id)
}


#' List Spark Sessions.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureSparkNewSession
#'
#' @family Spark functions
#' @export
azureSparkListSessions <- function(azureActiveContext, clustername, hdiAdmin,
                                   hdiPassword, verbose = FALSE) {
  HA = ""
  if (missing(clustername)) {
    CN <- azureActiveContext$clustername
  } else (CN = clustername)
  if (missing(hdiAdmin)) {
    HA <- azureActiveContext$hdiAdmin
  } else (HA = hdiAdmin)
  if (missing(hdiPassword)) {
    HP <- azureActiveContext$hdiPassword
  } else (HP = hdiPassword)
  verbosity <- set_verbosity(verbose)


  if (!length(CN)) {
    stop("Error: No Valid clustername provided")
  }
  if (!length(HA)) {
    stop("Error: No Valid hdiAdmin provided")
  }
  if (!length(HP)) {
    stop("Error: No Valid hdiPassword  provided")
  }

  azureActiveContext$hdiAdmin <- HA
  azureActiveContext$hdiPassword <- HP
  azureActiveContext$clustername <- CN

  URL <- paste("https://", CN, ".azurehdinsight.net/livy/sessions", sep = "")

  # URL <-
  # paste('https://management.azure.com/subscriptions/',subscriptionID,'/resourceGroups/',resourceGroup,'/providers/Microsoft.HDInsight/clusters/',clustername,'?api-version=2015-03-01-preview',sep='')

  # print(URL)

  r <- GET(URL, add_headers(.headers = c(`Content-type` = "application/json")),
           authenticate(HA, HP), verbosity)
  # ,authenticate('admin', 'Summer2014!')
  rl <- content(r, "text", encoding = "UTF-8")
  df <- fromJSON(rl)
  # print(df) print(df$sessions$appId)
  dfn <- as.data.frame(df$sessions$id)
  clust <- nrow(dfn)
  if (clust == 0)
    stop("No Sessions available")
  dfn[1:clust, 2] <- df$sessions$appId
  dfn[1:clust, 3] <- df$sessions$state
  dfn[1:clust, 4] <- df$sessions$proxyUser
  dfn[1:clust, 5] <- df$sessions$kind
  colnames(dfn) <- c("ID", "appID", "state", "proxyUser", "kind")

  return(dfn)
}


#' Stop a Spark Sessions.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureSparkNewSession
#'
#' @family Spark functions
#' @export
azureSparkStopSession <- function(azureActiveContext, clustername, hdiAdmin,
                                  hdiPassword, sessionID, verbose = FALSE) {

  azureCheckToken(azureActiveContext)

  if (missing(clustername)) {
    CN <- azureActiveContext$clustername
  } else (CN = clustername)
  if (missing(hdiAdmin)) {
    HA <- azureActiveContext$hdiAdmin
  } else (HA = hdiAdmin)
  if (missing(hdiPassword)) {
    HP <- azureActiveContext$hdiPassword
  } else (HP = hdiPassword)
  if (missing(sessionID)) {
    SI <- azureActiveContext$sessionID
  } else (SI = sessionID)
  verbosity <- set_verbosity(verbose)


  if (!length(CN)) {
    stop("Error: No clustername provided")
  }
  if (!length(HA)) {
    stop("Error: No hdiAdmin provided")
  }
  if (!length(HP)) {
    stop("Error: No hdiPassword  provided")
  }
  if (!length(SI)) {
    stop("Error: No sessionID provided")
  }

  azureActiveContext$hdiAdmin <- HA
  azureActiveContext$hdiPassword <- HP
  azureActiveContext$clustername <- CN
  azureActiveContext$sessionID <- SI

  URL <- paste("https://", CN, ".azurehdinsight.net/livy/sessions/",
               SI, sep = "")

  # URL <-
  # paste('https://management.azure.com/subscriptions/',subscriptionID,'/resourceGroups/',resourceGroup,'/providers/Microsoft.HDInsight/clusters/',clustername,'?api-version=2015-03-01-preview',sep='')

  # print(URL)

  r <- DELETE(URL, add_headers(.headers = c(`Content-type` = "application/json")),
              authenticate(HA, HP), verbosity)

  # rl <- content(r,'text',encoding='UTF-8') print(rl)
  if (status_code(r) == "404")
    stop(paste("sessionID not found (", status_code(r), ")"))

  if (status_code(r) != "200")
    stop(paste("Error Return Code:", status_code(r)))
  return(TRUE)
}


#' Send Spark Statements/comamnds (REPL/Interactive mode).
#'
#' @inheritParams setAzureContext
#' @inheritParams azureSparkNewSession
#'
#' @param CMD CMD
#'
#' @family Spark functions
#' @export
azureSparkCMD <- function(azureActiveContext, CMD, clustername, hdiAdmin,
                          hdiPassword, sessionID, verbose = FALSE) {

  if (missing(clustername)) {
    CN <- azureActiveContext$clustername
  } else (CN = clustername)
  if (missing(hdiAdmin)) {
    HA <- azureActiveContext$hdiAdmin
  } else (HA = hdiAdmin)
  if (missing(hdiPassword)) {
    HP <- azureActiveContext$hdiPassword
  } else (HP = hdiPassword)
  if (missing(sessionID)) {
    SI <- azureActiveContext$sessionID
  } else (SI = sessionID)
  if (missing(CMD)) {
    stop("Error: No CMD provided")
  }
  verbosity <- set_verbosity(verbose)


  if (!length(CN)) {
    stop("Error: No Valid clustername provided")
  }
  if (!length(HA)) {
    stop("Error: No Valid hdiAdmin provided")
  }
  if (!length(HP)) {
    stop("Error: No Valid hdiPassword  provided")
  }
  if (!length(SI)) {
    stop("Error: No sessionID provided")
  }

  azureActiveContext$hdiAdmin <- HA
  azureActiveContext$hdiPassword <- HP
  azureActiveContext$clustername <- CN
  azureActiveContext$sessionID <- SI

  URL <- paste("https://", CN, ".azurehdinsight.net/livy/sessions/",
               SI, "/statements", sep = "")

  # URL <-
  # paste('https://management.azure.com/subscriptions/',subscriptionID,'/resourceGroups/',resourceGroup,'/providers/Microsoft.HDInsight/clusters/',clustername,'?api-version=2015-03-01-preview',sep='')

  # print(URL) print(typeof(CMD))
  bodyI <- list(code = CMD)
  # print(CMD)

  r <- POST(URL, add_headers(.headers = c(`Content-type` = "application/json")),
            authenticate(HA, HP), body = bodyI, encode = "json", verbosity)

  rl <- content(r, "text", encoding = "UTF-8")
  rh <- headers(r)

  if (status_code(r) == "404")
    stop(paste("sessionID not found (", status_code(r), ")"))

  if (status_code(r) != "201")
    stop(paste("Error Return Code:", status_code(r)))

  df <- fromJSON(rl)
  # print(df$sessions$appId)
  if (df$state == "available") {
    RET <- df$output$data
    return(toString(RET))
  }
  DUR <- 2
  URL <- paste("https://", CN, ".azurehdinsight.net/livy/", rh$location,
               sep = "")
  # print(URL)
  message(paste("CMD Running: ", Sys.time()))
  message("Running(R) Waiting(W) Completed(C)")

  while (df$state == "running" || df$state == "waiting") {
    Sys.sleep(DUR)
    if (DUR < 5)
      DUR <- DUR + 1
    if (df$state == "running")
      message("R",appendLF = FALSE)
    if (df$state == "waiting")
      message("W",appendLF = FALSE)
    
    r <- GET(URL, add_headers(.headers = c(`Content-type` = "application/json")),
             authenticate(HA, HP))
    rl <- content(r, "text", encoding = "UTF-8")
    rh <- headers(r)
    df <- fromJSON(rl)

  }
  message("C",appendLF = FALSE)
  message("Finished Running statement: ", Sys.time())
  RET <- df$output$data[1]
  # rownames(RET) <- 'Return Value'
  return(toString(RET))

}


#' Submit Spark Job (Batch mode).
#'
#' @inheritParams setAzureContext
#' @inheritParams azureSparkNewSession
#'
#' @param FILE file
#' @param log log
#'
#' @family Spark functions
#' @export
azureSparkJob <- function(azureActiveContext, FILE, clustername, hdiAdmin,
                          hdiPassword, log = "URL", verbose = FALSE) {

  if (missing(clustername)) {
    CN <- azureActiveContext$clustername
  } else (CN = clustername)
  if (missing(hdiAdmin)) {
    HA <- azureActiveContext$hdiAdmin
  } else (HA = hdiAdmin)
  if (missing(hdiPassword)) {
    HP <- azureActiveContext$hdiPassword
  } else (HP = hdiPassword)
  if (missing(FILE)) {
    stop("Error: No CMD provided")
  }
  verbosity <- set_verbosity(verbose)


  if (!length(CN)) {
    stop("Error: No Valid clustername provided")
  }
  if (!length(HA)) {
    stop("Error: No Valid hdiAdmin provided")
  }
  if (!length(HP)) {
    stop("Error: No Valid hdiPassword  provided")
  }

  azureActiveContext$hdiAdmin <- HA
  azureActiveContext$hdiPassword <- HP
  azureActiveContext$clustername <- CN

  URL <- paste("https://", CN, ".azurehdinsight.net/livy/batches", sep = "")

  # URL <-
  # paste('https://management.azure.com/subscriptions/',subscriptionID,'/resourceGroups/',resourceGroup,'/providers/Microsoft.HDInsight/clusters/',clustername,'?api-version=2015-03-01-preview',sep='')

  # print(URL) print(typeof(CMD))
  bodyI <- list(file = FILE)
  # print(CMD)

  r <- POST(URL, add_headers(.headers = c(`Content-type` = "application/json")),
            authenticate(HA, HP), body = bodyI, encode = "json", verbosity)

  rl <- content(r, "text", encoding = "UTF-8")
  rh <- headers(r)

  if (status_code(r) == "404")
    stop(paste("sessionID not found (", status_code(r), ")"))

  if (status_code(r) != "201")
    stop(paste("Error Return Code:", status_code(r)))

  df <- fromJSON(rl)
  BI <- df$id
  # print(df$sessions$appId)
  if (df$state == "available")
    return(df$output$data)
  DUR <- 2
  BI <- df$id

  URL <- paste("https://", CN, ".azurehdinsight.net/livy/batches/", BI,
               sep = "")
  # print(URL)
  message(paste("CMD Running: ", Sys.time()))
  message("Running(R), Completed(C)")
  LOGURL2 <- ""

  while (df$state == "running") {
    Sys.sleep(DUR)
    if (DUR < 5)
      DUR <- DUR + 1
    message("R")
    r <- GET(URL, add_headers(.headers = c(`Content-type` = "application/json")),
             authenticate(HA, HP), verbosity)
    rl <- content(r, "text", encoding = "UTF-8")
    rh <- headers(r)
    df <- fromJSON(rl)
    if (length(df$appInfo$driverlogUrl))
      LOGURL2 <- df$appInfo$driverlogUrl
  }
  message("C")
  STATE <- df$state
  message("")
  message(paste("Finished Running statement: ", Sys.time()))

  # BID = gsub('application_','container_',df$appId) print(df$log[2])
  # HN<- strsplit(df$log[2], ' ')
  print("LOGURL")
  print(LOGURL2)

  if (log == "URL")
    azureActiveContext$log <- LOGURL2 else {
      print("LOGURL")
      print(LOGURL2)

      r <- GET(LOGURL2, add_headers(.headers = c(`Content-type` = "application/json")),
               authenticate(HA, HP), verbosity)
      rl <- content(r, "text", encoding = "UTF-8")
      azureActiveContext$log <- rl
    }
  return(STATE)
}


#' List Spark Jobs (Batch mode).
#'
#' @inheritParams setAzureContext
#' @inheritParams azureSparkNewSession
#'
#' @family Spark functions
#' @return manually direct output to blob fule /SQL in script
#' @export
azureSparkListJobs <- function(azureActiveContext, clustername, hdiAdmin,
                               hdiPassword, verbose = FALSE) {
  HA = ""
  if (missing(clustername)) {
    CN <- azureActiveContext$clustername
  } else (CN = clustername)
  if (missing(hdiAdmin)) {
    HA <- azureActiveContext$hdiAdmin
  } else (HA = hdiAdmin)
  if (missing(hdiPassword)) {
    HP <- azureActiveContext$hdiPassword
  } else (HP = hdiPassword)
  verbosity <- set_verbosity(verbose)


  if (!length(CN)) {
    stop("Error: No Valid clustername provided")
  }
  if (!length(HA)) {
    stop("Error: No Valid hdiAdmin provided")
  }
  if (!length(HP)) {
    stop("Error: No Valid hdiPassword  provided")
  }

  azureActiveContext$hdiAdmin <- HA
  azureActiveContext$hdiPassword <- HP
  azureActiveContext$clustername <- CN

  URL <- paste("https://", CN, ".azurehdinsight.net/livy/batches", sep = "")

  # URL <-
  # paste('https://management.azure.com/subscriptions/',subscriptionID,'/resourceGroups/',resourceGroup,'/providers/Microsoft.HDInsight/clusters/',clustername,'?api-version=2015-03-01-preview',sep='')

  # print(URL)


  r <- GET(URL, add_headers(.headers = c(`Content-type` = "application/json")),
           authenticate(HA, HP), verbosity)
  # ,authenticate('admin', 'Summer2014!')
  rl <- content(r, "text", encoding = "UTF-8")
  df <- fromJSON(rl)
  # print(df$sessions$id)
  print(colnames(df))
  dfn <- as.data.frame(df$sessions$id)
  clust <- nrow(dfn)
  if (clust == 0)
    stop("No Sessions available")
  dfn[1:clust, 2] <- df$sessions$appId
  dfn[1:clust, 3] <- df$sessions$state

  colnames(dfn) <- c("ID", "appID", "state")

  return(dfn)
}


#' Show Spark log Output.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureSparkNewSession
#'
#' @param URL URL
#'
#' @family Spark
#'
#' @family Spark functions
#' @export
azureSparkShowURL <- function(azureActiveContext, URL) {
  if (!missing(URL))
    browseURL(URL) else browseURL(azureActiveContext$log)
  return(TRUE)
}
