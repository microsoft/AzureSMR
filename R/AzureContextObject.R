#' Create an AzureContext
#'
#' Create a container(AzureContext) for holding variables used by the AzureSMR package.
#' If the Tenant ID, Client ID and Authenication Key is provided the function will attempt to
#' authenicate the session.  
#'
#' @inheritParams setAzureContext
#' @family Context Object
#' @note See \url{https://azure.microsoft.com/en-us/documentation/articles/resource-group-create-service-principal-portal/} to learn how to set up an Active directory application
#' @references \url{https://azure.microsoft.com/en-us/documentation/articles/resource-group-create-service-principal-portal/}
#' @export
createAzureContext <- function(tenantID, clientID, authKey){
  AzEnv <- new.env(parent = globalenv())
  if (!missing(tenantID)) AzEnv$tenantID <- tenantID else  AzEnv$tenantID <- "?"
  if (!missing(clientID)) AzEnv$clientID <- clientID else  AzEnv$tenantID <- "?"
  if (!missing(authKey)) AzEnv$authKey <- authKey else  AzEnv$tenantID <- "?"
  
  if (!missing(tenantID) && !missing(clientID) && !missing(authKey) )
      azureAuthenticate(AzEnv,tenantID, clientID, authKey)
  return(AzEnv)
}

#' Dumps out the contents of the AzureContext.
#'
#' @inheritParams setAzureContext
#' @family Context Object
#' @export

dumpAzureContext <- function(azureActiveContext){
  ls.str(envir = azureActiveContext)
}


#' Updates the value of an AzureContext variable.
#'
#' @param azureActiveContext A container used for caching variables used by AzureSMR
#' @param tenantID The Tenant ID which was provided when the Active Directory application / service principal is created
#' @param clientID The Client ID which was provided when the Active Directory application / service principal is created
#' @param authKey The Authentication Key which was provided when the Active Directory application / service principal is created
#' @param subscriptionID Set the subscriptionID Obtained automatically by AzureAuthenticate when only one is available
#' @param azToken Azure Token Object - Obtained by AzureAuthenticate
#' @param resourceGroup Set the name of the Resource Group
#' @param vmName Set the name of the virtual Machine
#' @param storageAccount Set the name of the azure storage account
#' @param storageKey Set the Storage Key associated with storage account
#' @param blob Set the blob name 
#' @param clustername Set the clustername
#' @param sessionID Set the Spark sessionID
#' @param hdiAdmin Set the HDInsight admin username
#' @param hdiPassword  Set the HDInsight admin Password
#' @param container Set the storage container 
#' @param kind Set the HDinsight kind "hadoop","spark" or "pyspark"
#
# @param log log Object#'
#'
#' @export
setAzureContext <- function(azureActiveContext,tenantID, clientID, authKey,azToken,
                            subscriptionID,resourceGroup,
                            storageKey,storageAccount,
                            container,blob,
                            vmName,
                            hdiAdmin,hdiPassword ,clustername,kind,sessionID)
{
  if (!missing(tenantID)) azureActiveContext$tenantID <- tenantID
  if (!missing(clientID)) azureActiveContext$clientID <- clientID
  if (!missing(authKey)) azureActiveContext$authKey <- authKey
  if (!missing(azToken)) azureActiveContext$AZtoken <- azToken
  if (!missing(subscriptionID)) azureActiveContext$subscriptionID <- subscriptionID
  if (!missing(resourceGroup)) azureActiveContext$resourceGroup <- resourceGroup
  if (!missing(storageKey)) azureActiveContext$storageKey <- storageKey
  if (!missing(storageAccount)) azureActiveContext$storageAccount <- storageAccount
  if (!missing(container)) azureActiveContext$container <- container
  if (!missing(blob)) azureActiveContext$container <- blob
  if (!missing(vmName)) azureActiveContext$vmName <- vmName
  if (!missing(clustername)) azureActiveContext$clustername <- clustername
  if (!missing(hdiAdmin)) azureActiveContext$hdiAdmin <- hdiAdmin
  if (!missing(hdiPassword)) azureActiveContext$hdiPassword  <- hdiPassword 
  if (!missing(kind)) azureActiveContext$kind <- kind
  if (!missing(sessionID)) azureActiveContext$sessionID <- sessionID
}
