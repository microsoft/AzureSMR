#' List VMs in a Subscription.
#'
#' @inheritParams SetAzureContext
#' @param ResourceGroup ResourceGroup Object (or use AzureActiveContext)
#' @param Location Azure Resource Location
#' @param AzToken Token Object (or use AzureActiveContext)
#' @param SubscriptionID SubscriptionID Object (or use AzureActiveContext)
#' @param verbose Print Tracing information (Default False)
#'
#' @family Virtual Machine
#' @export
AzureListVM <- function(AzureActiveContext,ResourceGroup, Location,SubscriptionID,
                        AzToken,verbose=FALSE) {
  AzureCheckToken(AzureActiveContext)
  if(missing(AzToken)) {AT <- AzureActiveContext$Token} else (AT = AzToken)
  if(missing(SubscriptionID)) {SUBIDI <- AzureActiveContext$SubscriptionID} else (SUBIDI = SubscriptionID)
  if(missing(ResourceGroup)) {RGI <- AzureActiveContext$ResourceGroup} else (RGI = ResourceGroup)
  verbosity <- if(verbose) httr::verbose(TRUE) else NULL

  if (!length(RGI)) {stop("Error: No ResourceGroup provided: Use ResourceGroup argument or set in AzureContext")}
  if (!length(SUBIDI)) {stop("Error: No SubscriptionID provided: Use SUBID argument or set in AzureContext")}
  if (!length(AT)) {stop("Error: No Token / Not currently Authenticated")}

  URL <- paste("https://management.azure.com/subscriptions/",SUBIDI,"/resourceGroups/",RGI,"/providers/Microsoft.Compute/virtualmachines?api-version=2015-05-01-preview",sep="")

  r <- GET(URL,add_headers(.headers = c("Host" = "management.azure.com" ,"Authorization" = AT, "Content-Type" = "application/json")),verbosity)
  rl <- content(r,"text",encoding="UTF-8")
  df <- fromJSON(rl)
  # print(df)
  dfn <- as.data.frame(df$value$name)
  clust <- nrow(dfn)
  if (clust < 1)
  {
    warning("No Virtual Machines found")
    return(NULL)
  }
  dfn[1:clust,1] <- df$value$name
  dfn[1:clust,2] <- df$value$location
  dfn[1:clust,3] <- df$value$type
  dfn[1:clust,4] <- df$value$properties$storageProfile$osDisk$osType
  dfn[1:clust,5] <- df$value$properties$provisioningState
  dfn[1:clust,6] <- df$value$properties$osProfile$adminUsername
  dfn[1:clust,7] <- df$value$id
  #dfn[1:clust,8] <- df$value$properties.vmSize
#  print(df$value$properties.vmSize)

  #dvn

  colnames(dfn) <- c("Name","Location", "Type", "OS","State","Admin", "ID")
  AzureActiveContext$SubscriptionID <- SUBIDI
  AzureActiveContext$ResourceGroup <- RGI

  return(dfn)
}


#' Start a Virtual Machine.
#'
#' @inheritParams AzureListVM
#' @param VMName Virtual Machine Name
#' @param Mode Wait for operation to complete "Sync" (Default)
#'
#' @family Virtual Machine
#' @export
AzureStartVM <- function(AzureActiveContext,ResourceGroup, VMName, Mode="Sync",SubscriptionID,AzToken,verbose=FALSE) {
  AzureCheckToken(AzureActiveContext)
  if(missing(AzToken)) {AT <- AzureActiveContext$Token} else (AT = AzToken)
  if(missing(SubscriptionID)) {SUBIDI <- AzureActiveContext$SubscriptionID} else (SUBIDI = SubscriptionID)
  if(missing(ResourceGroup)) {RGI <- AzureActiveContext$ResourceGroup} else (RGI = ResourceGroup)
  if(missing(VMName)) {VMNameI <- AzureActiveContext$VMNameI} else (VMNameI = VMName)
  verbosity <- if(verbose) httr::verbose(TRUE) else NULL

  if (!length(RGI)) {stop("Error: No ResourceGroup provided: Use ResourceGroup argument or set in AzureContext")}
  if (!length(SUBIDI)) {stop("Error: No SubscriptionID provided: Use SUBID argument or set in AzureContext")}
  if (!length(AT)) {stop("Error: No Token / Not currently Authenticated")}
  if (!length(VMNameI)) {stop("No VM Name provided")}

  URL <- paste("https://management.azure.com/subscriptions/",SUBIDI,"/resourceGroups/",RGI,"/providers/Microsoft.Compute/virtualmachines/",VMNameI,"/start?api-version=2015-05-01-preview",sep="")
  #print(URL)

  r <- POST(URL,add_headers(.headers = c("Host" = "management.azure.com" ,"Authorization" = AT, "Content-Type" = "application/json")),verbosity)
  if (status_code(r) == 404) {stop(paste("Error: Return code",status_code(r) )," (Not Found)")}
  if (status_code(r) != 200 && status_code(r) != 202 ) {stop(paste("Error: Return code",status_code(r) ))}
  rl <- content(r,"text",encoding="UTF-8")

  #print(rl)
  #df <- fromJSON(rl)
  #dfn <- as.data.frame(df$value$name)
  AzureActiveContext$SubscriptionID <- SUBIDI
  AzureActiveContext$ResourceGroup <- RGI
  AzureActiveContext$VMName <- VMNameI
  if(toupper(Mode) == "SYNC")
  {
    rc="running"
    writeLines(paste("AzureStartVM: Request Submitted: ",Sys.time()))
    writeLines("Updating(U), deallocating(D), starting(S), Deallocated(-) ")
    a=1
    Sys.sleep(5)
    while (a>0)
    {
      rc1 <- AzureVMStatus(AzureActiveContext)
      #rc1 <- rc$displayStatus[2]
#      print(rc)
#      print(rc1)
      a=a+1
      if (grepl("running",rc1)) {
        writeLines("")
        writeLines(paste("Finished Started Sucessfully: ",Sys.time()))
        break()
      }
      if (grepl("Updating",rc1)) {rc1<-"U"}
      else if (grepl("deallocating",rc1)) {rc1<-"D"}
      else if (grepl("running",rc1)) {rc1<-"R"}
      else if (grepl("starting",rc1)) {rc1<-"S"}
      else if (grepl("updating",rc1)) {rc1<-"U"}
      else if (grepl("deallocated",rc1)) {rc1<-"-"}

      if (is.na(rc1)) rc1 <- "."
      if (rc1 == "NA") rc1 <- "*"

      cat(rc1)

      if( a > 500) break()
      Sys.sleep(5)
    }
  }
  return("Done")
}


#' Stop a Virtual Machine.
#'
#' @inheritParams SetAzureContext
#' @inheritParams AzureListVM
#' @inheritParams AzureStartVM
#'
#' @family Virtual Machine
#' @export
AzureStopVM <- function(AzureActiveContext,ResourceGroup, VMName,Mode="Sync",SubscriptionID,AzToken,verbose=FALSE) {
  AzureCheckToken(AzureActiveContext)

  if(missing(AzToken)) {AT <- AzureActiveContext$Token} else (AT = AzToken)
  if(missing(SubscriptionID)) {SUBIDI <- AzureActiveContext$SubscriptionID} else (SUBIDI = SubscriptionID)
  if(missing(ResourceGroup)) {RGI <- AzureActiveContext$ResourceGroup} else (RGI = ResourceGroup)
  if(missing(VMName)) {VMNameI <- AzureActiveContext$VMName} else (VMNameI = VMName)
  verbosity <- if(verbose) httr::verbose(TRUE) else NULL

  if (!length(RGI)) {stop("Error: No ResourceGroup provided: Use ResourceGroup argument or set in AzureContext")}
  if (!length(SUBIDI)) {stop("Error: No SubscriptionID provided: Use SUBID argument or set in AzureContext")}
  if (!length(AT)) {stop("Error: No Token / Not currently Authenticated")}
  if (!length(VMNameI)) {stop("No VM Name provided")}

  URL <- paste("https://management.azure.com/subscriptions/",SUBIDI,"/resourceGroups/",RGI,"/providers/Microsoft.Compute/virtualmachines/",VMNameI,"/deallocate?api-version=2015-05-01-preview",sep="")
  # print(URL)

  r <- POST(URL,add_headers(.headers = c("Host" = "management.azure.com" ,"Authorization" = AT, "Content-Type" = "application/json")),verbosity)
  if (status_code(r) == 404) {stop(paste("Error: Return code",status_code(r) )," (Not Found)")}
  if (status_code(r) != 200 && status_code(r) != 202 ) {stop(paste("Error: Return code",status_code(r) ))}

  rl <- content(r,"text",encoding="UTF-8")
  #df <- fromJSON(rl)

  #dfn <- as.data.frame(df$value$name)
  AzureActiveContext$SubscriptionID <- SUBIDI
  AzureActiveContext$ResourceGroup <- RGI
  AzureActiveContext$VMName <- VMNameI

  if(toupper(Mode) == "SYNC")
  {
    rc="running"
    writeLines(paste("AzureStopVM: Request Submitted: ",Sys.time()))
    writeLines("Updating(U), deallocating(D), starting(S), Stopped/Deallocated(-) ")
    a=1
    while (a>0)
    {
      rc1 <- AzureVMStatus(AzureActiveContext)
#      rc1 <- rc$displayStatus[2]
      #      cat(paste(rc," "))
      if (grepl("deallocated",rc1)) {
        writeLines("")
        writeLines(paste("Finished Deallocated Sucessfully: ",Sys.time()))
        break()
      }

      a=a+1
      if (grepl("deallocating",rc1)) {rc1<-"D"}
      else if (grepl("running",rc1)) {rc1<-"R"}
      else if (grepl("updating",rc1)) {rc1<-"U"}
      else if (grepl("Starting",rc1)) {rc1<-"S"}
      else if (grepl("Stopped",rc1)) {rc1<-"-"}
      else if (grepl("deallocated",rc1)) {rc1<-"-"}

      cat(rc1)

      if( a > 500) break()
      Sys.sleep(5)
    }
  }
  writeLines(paste("Finished: ",Sys.time()))
  return(rc1)
}


#' Get Status of a Virtual Machine.
#'
#' @inheritParams SetAzureContext
#' @inheritParams AzureListVM
#' @inheritParams AzureStartVM
#' @param Ignore Ignore
#'
#' @family Virtual Machine
#' @export
AzureVMStatus <- function(AzureActiveContext,ResourceGroup, VMName,
                          SubscriptionID,AzToken,Ignore="N",verbose=FALSE) {
  AzureCheckToken(AzureActiveContext)

  if(missing(AzToken)) {AT <- AzureActiveContext$Token} else (AT = AzToken)
  if(missing(SubscriptionID)) {SUBIDI <- AzureActiveContext$SubscriptionID} else (SUBIDI = SubscriptionID)
  if(missing(ResourceGroup)) {RGI <- AzureActiveContext$ResourceGroup} else (RGI = ResourceGroup)
  if(missing(VMName)) {VMNameI <- AzureActiveContext$VMName} else (VMNameI = VMName)
  verbosity <- if(verbose) httr::verbose(TRUE) else NULL

  if (!length(RGI)) {stop("Error: No ResourceGroup provided: Use ResourceGroup argument or set in AzureContext")}
  if (!length(SUBIDI)) {stop("Error: No SubscriptionID provided: Use SUBID argument or set in AzureContext")}
  if (!length(AT)) {stop("Error: No Token / Not currently Authenticated")}
  if (!length(VMNameI)) {stop("No VM Name provided")}

  URL <- paste("https://management.azure.com/subscriptions/",SUBIDI,"/resourceGroups/",RGI,"/providers/Microsoft.Compute/virtualmachines/",VMNameI,"/InstanceView?api-version=2015-05-01-preview",sep="")
  #  print(URL)

  r <- GET(URL,add_headers(.headers = c("Host" = "management.azure.com" ,"Authorization" = AT, "Content-Type" = "application/json")),verbosity)
  rl <- content(r,"text",encoding="UTF-8")
  df <- fromJSON(rl)
  #print(df)
  if (length(df$error$code) && df$error$code == "ExpiredAuthenticationToken")
    stop("Authentication token has expired. Run AzureAuthenticate() to renew.")

  dfn <- as.data.frame(df$statuses)

  clust <- nrow(dfn)
  if (clust < 1 && Ignore == "Y") return("NA")
  if (clust < 1) stop("No Virtual Machines found")
  return(paste(df$statuses$displayStatus,collapse=', '))

  return("Submitted")
}


#' Delete a Virtual Machine.
#'
#' @inheritParams AzureListVM
#' @inheritParams AzureStartVM
#' @family Virtual Machine
#' @export
AzureDeleteVM <- function(AzureActiveContext,ResourceGroup, VMName,SubscriptionID,AzToken,Mode="Sync",verbose=FALSE) {
  AzureCheckToken(AzureActiveContext)
  if(missing(AzToken)) {AT <- AzureActiveContext$Token} else (AT = AzToken)
  if(missing(SubscriptionID)) {SUBIDI <- AzureActiveContext$SubscriptionID} else (SUBIDI = SubscriptionID)
  if(missing(ResourceGroup)) {RGI <- AzureActiveContext$ResourceGroup} else (RGI = ResourceGroup)
  if(missing(VMName)) {VMNameI <- AzureActiveContext$VMNameI} else (VMNameI = VMName)
  verbosity <- if(verbose) httr::verbose(TRUE) else NULL

  if (!length(RGI)) {stop("Error: No ResourceGroup provided: Use ResourceGroup argument or set in AzureContext")}
  if (!length(SUBIDI)) {stop("Error: No SubscriptionID provided: Use SUBID argument or set in AzureContext")}
  if (!length(AT)) {stop("Error: No Token / Not currently Authenticated")}
  if (!length(VMNameI)) {stop("No VM Name provided")}

  URL <- paste("https://management.azure.com/subscriptions/",SUBIDI,"/resourceGroups/",RGI,"/providers/Microsoft.Compute/virtualmachines/",VMNameI,"?api-version=2015-05-01-preview",sep="")
  # print(URL)
  print (URL)

  r <- DELETE(URL,add_headers(.headers = c("Host" = "management.azure.com" ,"Authorization" = AT, "Content-Type" = "application/json")),verbosity)
  if (status_code(r) == 404) {stop(paste("Error: Return code",status_code(r) )," (Not Found)")}
  if (status_code(r) != 200 && status_code(r) != 202 ) {stop(paste("Error: Return code",status_code(r) ))}

  rl <- content(r,"text",encoding="UTF-8")
  print(rl)
  #df <- fromJSON(rl)

  #dfn <- as.data.frame(df$value$name)
  AzureActiveContext$SubscriptionID <- SUBIDI
  AzureActiveContext$ResourceGroup <- RGI
  AzureActiveContext$VMName <- VMNameI

  if(toupper(Mode) == "SYNC")
  {
    rc="running"
    writeLines(paste("AzureDeleteVM: Request Submitted: ",Sys.time()))
    writeLines("Updating(U), Deleting(D), Stopped/Deallocated(-) ")
    a=1
    while (a>0)
    {
      rc <- AzureVMStatus(Ignore="Y")
      #      cat(paste(rc," "))
      if (grepl("NA",rc)) {
        writeLines("")
        writeLines(paste("Finished Deleted Sucessfully: ",Sys.time()))
        break()
      }

      a=a+1
      if (grepl("Deleting",rc)) {rc<-"D"}
      else if (grepl("running",rc)) {rc<-"R"}
      else if (grepl("updating",rc)) {rc<-"U"}
      else if (grepl("Stopped",rc)) {rc<-"-"}
      else if (grepl("deallocated",rc)) {rc<-"-"}

      cat(rc)

      if( a > 500) break()
      Sys.sleep(5)
    }
  }
  writeLines(paste("Finished: ",Sys.time()))
  return(rc)
}
