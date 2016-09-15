#' @name AzureSM: AzureDeployTemplate
#' @title Submit a Azure Resource ManagerTemplate
#' @param AzureActiveContext Azure Context Object
#' @param DeplName DeplName
#' @param TemplateURL TemplateURL
#' @param ParamURL ParamURL
#' @param ResourceGroup ResourceGroup Object (or use AzureActiveContext)
#' @param Token Token Object (or use AzureActiveContext)
#' @param SubscriptionID SubscriptionID Object (or use AzureActiveContext)
#' @param verbose Print Tracing information (Default False)
#' @rdname AzureDeployTemplate
#' @export
AzureDeployTemplate <- function(AzureActiveContext,DeplName,TemplateURL,ParamURL,TemplateJSON,ParamJSON,Mode="Sync",ResourceGroup,SubscriptionID,AzToken, verbose = FALSE) {
  AzureCheckToken(AzureActiveContext)

  if(missing(AzToken)) {AT <- AzureActiveContext$Token} else (AT = AzToken)
  if(missing(SubscriptionID)) {SUBIDI <- AzureActiveContext$SubscriptionID} else (SUBIDI = SubscriptionID)
  if(missing(ResourceGroup)) {RGI <- AzureActiveContext$ResourceGroup} else (RGI = ResourceGroup)

  if (!length(RGI)) {stop("Error: No ResourceGroup provided: Use ResourceGroup argument or set in AzureContext")}
  if (!length(SUBIDI)) {stop("Error: No SubscriptionID provided: Use SUBID argument or set in AzureContext")}
  if (!length(AT)) {stop("Error: No Token / Not currently Authenticated")}
  if (!length(DeplName)) {stop("No DeplName provided")}

  if (missing(TemplateURL) && missing(TemplateJSON))
    {stop("No TemplateURL or TemplateJSON provided")}

    verbosity <- if(verbose) httr::verbose(TRUE) else NULL

  URL <- paste("https://management.azure.com/subscriptions/",SUBIDI,"/resourceGroups/",RGI,"/providers/microsoft.resources/deployments/",DeplName,"?api-version=2016-06-01",sep="")
 # print(URL)

  if (missing(TemplateJSON))
  {
    if (missing(ParamURL))
    {
      if (missing(ParamJSON))
        bodyI <- paste("{\"properties\": {\"templateLink\": { \"uri\": \"",TemplateURL,"\",\"contentVersion\": \"1.0.0.0\"},\"mode\": \"Incremental\",\"debugSetting\": {\"detailLevel\": \"requestContent, responseContent\"}}}",sep="")
      else
        bodyI <- paste("{\"properties\": {",ParamJSON,",\"templateLink\": { \"uri\": \"",TemplateURL,"\",\"contentVersion\": \"1.0.0.0\"},\"mode\": \"Incremental\",\"debugSetting\": {\"detailLevel\": \"requestContent, responseContent\"}}}",sep="")
    }
    else
        bodyI <- paste("{\"properties\": {\"templateLink\": { \"uri\": \"",TemplateURL,"\",\"contentVersion\": \"1.0.0.0\"},  \"mode\": \"Incremental\",  \"parametersLink\": {\"uri\": \"",ParamURL,"\",\"contentVersion\": \"1.0.0.0\"},\"debugSetting\": {\"detailLevel\": \"requestContent, responseContent\"}}}",sep="")
  }
  else
  {
    if (missing(ParamURL))
    {
      if (missing(ParamJSON))
        bodyI <- paste("{\"properties\": {\"template\": ",TemplateJSON,",\"mode\": \"Incremental\",\"debugSetting\": {\"detailLevel\": \"requestContent, responseContent\"}}}",sep="")
      else
        bodyI <- paste("{\"properties\": {",ParamJSON,",\"template\": ",TemplateJSON,",\"mode\": \"Incremental\",\"debugSetting\": {\"detailLevel\": \"requestContent, responseContent\"}}}",sep="")
    }
    else
      bodyI <- paste("{\"properties\": {\"template\": ",TemplateJSON,",  \"mode\": \"Incremental\",  \"parametersLink\": {\"uri\": \"",ParamURL,"\",\"contentVersion\": \"1.0.0.0\"},\"debugSetting\": {\"detailLevel\": \"requestContent, responseContent\"}}}",sep="")
}

  r <- PUT(URL,add_headers(.headers = c("Host" = "management.azure.com" ,"Authorization" = AT, "Content-Type" = "application/json")),body=bodyI,verbosity)
#  print(paste(DeplName,"Submitted"))
  if (status_code(r) != 200 && status_code(r) != 202 ) {stop(paste("Error: Return code",status_code(r) ))}
    rl <- content(r,"text",encoding="UTF-8")
#  print (rl)
  df <- fromJSON(rl)
  if(toupper(Mode) == "SYNC")
  {
    rc="running"
    writeLines(paste("AzureDeployTemplate: Request Submitted: ",Sys.time()))
    writeLines("Running(R), Succeeded(S)")
    a=1
    while (a>0)
    {
      rc <- AzureDeployStatus(AzureActiveContext,DeplName=DeplName,ResourceGroup=RGI)
      #      cat(paste(rc," "))
      if (grepl("Succeeded",rc)){
        writeLines("")
        writeLines(paste("Finished Deploying Sucessfully: ",Sys.time()))
        break()
      }
      if (grepl("Error",rc)){
        writeLines("")
        writeLines(paste("Error Deploying: ",Sys.time()))
        break()
      }

      a=a+1
      if (grepl("Succeeded",rc)) {rc<-"S"}
      else if (grepl("Running",rc)) {rc<-"R"}
      else if (grepl("updating",rc)) {rc<-"U"}
      else if (grepl("Starting",rc)) {rc<-"S"}
      else if (grepl("Accepted",rc)) {rc<-"A"}

      cat(rc)

      if( a > 500) break()
      Sys.sleep(5)
    }
  }
  writeLines(paste("Deployment",DeplName,"Submitted: ",Sys.time()))
  return("OK")
}

#' @name AzureSM: AzureDeployStatus
#' @title Check Template DeployStatus
#' @param AzureActiveContext Azure Context Object
#' @param DeplName DeplName
#' @param ResourceGroup ResourceGroup Object (or use AzureActiveContext)
#' @param Token Token Object (or use AzureActiveContext)
#' @param SubscriptionID SubscriptionID Object (or use AzureActiveContext)
#' @param Verbose Print Tracing information (Default False)
#' @export
AzureDeployStatus <- function(AzureActiveContext,DeplName,ResourceGroup, SubscriptionID,AzToken,verbose = FALSE) {
  AzureCheckToken(AzureActiveContext)
  if(missing(AzToken)) {AT <- AzureActiveContext$Token} else (AT = AzToken)
  if(missing(SubscriptionID)) {SUBIDI <- AzureActiveContext$SubscriptionID} else (SUBIDI = SubscriptionID)
  if(missing(ResourceGroup)) {RGI <- AzureActiveContext$ResourceGroup} else (RGI = ResourceGroup)
  verbosity <- if(verbose) httr::verbose(TRUE) else NULL

  if (!length(RGI)) {stop("Error: No ResourceGroup provided: Use ResourceGroup argument or set in AzureContext")}
  if (!length(SUBIDI)) {stop("Error: No SubscriptionID provided: Use SUBID argument or set in AzureContext")}
  if (!length(AT)) {stop("Error: No Token / Not currently Authenticated")}
  if (!length(DeplName)) {stop("No DeplName provided")}

  URL <- paste("https://management.azure.com/subscriptions/",SUBIDI,"/resourceGroups/",RGI,"/providers/microsoft.resources/deployments/",DeplName,"?api-version=2016-06-01",sep="")
  #  print(URL)

  r <- GET(URL,add_headers(.headers = c("Host" = "management.azure.com" ,"Authorization" = AT, "Content-Type" = "application/json")),verbosity)
  rl <- content(r,"text",encoding="UTF-8")

  df <- fromJSON(rl)
  #print(df)
  return(df$properties$provisioningState)
}

#' @name AzureSM: AzureDeleteDeploy
#' @title Delete Template Deployment
#' @param AzureActiveContext Azure Context Object
#' @param DeplName DeplName
#' @param ResourceGroup ResourceGroup Object (or use AzureActiveContext)
#' @param Token Token Object (or use AzureActiveContext)
#' @param SubscriptionID SubscriptionID Object (or use AzureActiveContext)
#' @param Verbose Print Tracing information (Default False)
#' @export
AzureDeleteDeploy <- function(AzureActiveContext,DeplName,ResourceGroup,SubscriptionID,AzToken,verbose = FALSE) {
  AzureCheckToken(AzureActiveContext)

  if(missing(AzToken)) {AT <- AzureActiveContext$Token} else (AT = AzToken)
  if(missing(SubscriptionID)) {SUBIDI <- AzureActiveContext$SubscriptionID} else (SUBIDI = SubscriptionID)
  if(missing(ResourceGroup)) {RGI <- AzureActiveContext$ResourceGroup} else (RGI = ResourceGroup)
  verbosity <- if(verbose) httr::verbose(TRUE) else NULL

  if (!length(RGI)) {stop("Error: No ResourceGroup provided: Use ResourceGroup argument or set in AzureContext")}
  if (!length(SUBIDI)) {stop("Error: No SubscriptionID provided: Use SUBID argument or set in AzureContext")}
  if (!length(AT)) {stop("Error: No Token / Not currently Authenticated")}
  if (!length(DeplName)) {stop("No DeplName provided")}

  URL <- paste("https://management.azure.com/subscriptions/",SUBIDI,"/resourceGroups/",RGI,"/providers/microsoft.resources/deployments/",DeplName,"?api-version=2016-06-01",sep="")
  #  print(URL)

  r <- DELETE(URL,add_headers(.headers = c("Host" = "management.azure.com" ,"Authorization" = AT, "Content-Type" = "application/json")))
  print(http_status(r))
  rl <- content(r,"text",encoding="UTF-8")
  print(rl)
  df <- fromJSON(rl)
  print(df)
  return("OK")
}

AzureCancelDeploy <- function(AzureActiveContext,DeplName,ResourceGroup, SubscriptionID,AzToken,verbose = FALSE) {

  AzureCheckToken(AzureActiveContext)

  if(missing(AzToken)) {AT <- AzureActiveContext$Token} else (AT = AzToken)
  if(missing(SubscriptionID)) {SUBIDI <- AzureActiveContext$SubscriptionID} else (SUBIDI = SubscriptionID)
  if(missing(ResourceGroup)) {RGI <- AzureActiveContext$ResourceGroup} else (RGI = ResourceGroup)
  verbosity <- if(verbose) httr::verbose(TRUE) else NULL

  if (!length(RGI)) {stop("Error: No ResourceGroup provided: Use ResourceGroup argument or set in AzureContext")}
  if (!length(SUBIDI)) {stop("Error: No SubscriptionID provided: Use SUBID argument or set in AzureContext")}
  if (!length(AT)) {stop("Error: No Token / Not currently Authenticated")}
  if (!length(DeplName)) {stop("No DeplName provided")}

  URL <- paste("https://management.azure.com/subscriptions/",SUBIDI,"/resourceGroups/",RGI,"/providers/microsoft.resources/deployments/",DeplName,"/cancel?api-version=2016-06-01",sep="")
  #  print(URL)

  r <- POST(URL,add_headers(.headers = c("Host" = "management.azure.com" ,"Authorization" = AT, "Content-Type" = "application/json")),verbosity)
  rl <- content(r,"text",encoding="UTF-8")
  df <- fromJSON(rl)
  return(df$category)
}
