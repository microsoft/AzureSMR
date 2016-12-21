#' Create an AzureContext.
#'
#' Create a container (azureContextObject) for holding variables used by the AzureSMR package.  If the Tenant ID, Client ID and Authenication Key are provided the function will attempt to authenticate the session.
#'
#' See the Azure documentation (\url{https://azure.microsoft.com/en-us/documentation/articles/resource-group-create-service-principal-portal/}) for information to configure an Active Directory application.
#'
#' @inheritParams setAzureContext
#' @family azureActiveContext functions
#'
#' @seealso \code{\link{setAzureContext}}
#' @export
createAzureContext <- function(tenantID, clientID, authKey){
  azEnv <- new.env(parent = globalenv())
  azEnv <- as.azureActiveContext(azEnv)

  if (!missing(tenantID)) azEnv$tenantID <- tenantID else  azEnv$tenantID <- "?"
  if (!missing(clientID)) azEnv$clientID <- clientID else  azEnv$tenantID <- "?"
  if (!missing(authKey)) azEnv$authKey <- authKey else  azEnv$tenantID <- "?"

  if (!missing(tenantID) && !missing(clientID) && !missing(authKey) )
      azureAuthenticate(azEnv,tenantID, clientID, authKey)
  return(azEnv)
}

#' Dumps out the contents of the AzureContext.
#'
#' @inheritParams setAzureContext
#' @export

dumpAzureContext <- function(azureActiveContext){
  .Deprecated(str)
}

#' Updates azureActiveContext object.
#'
#' Updates the value of an azureActiveContext object, created by \code{\link{createAzureContext}}
#'
#' @param azureActiveContext A container used for caching variables used by AzureSMR
#' @param tenantID The Tenant ID provided during creation of the Active Directory application / service principal
#' @param clientID The Client ID provided during creation of the Active Directory application / service principal
#' @param authKey The Authentication Key provided during creation of the Active Directory application / service principal
#' @param subscriptionID Set the subscriptionID.  This is obtained automatically by \code{\link{azureAuthenticate}} when only a single subscriptionID is available via Active Directory
#' @param azToken Azure authentication token, obtained by \code{\link{azureAuthenticate}}
#' @param resourceGroup Name of the resource group
#' @param vmName Name of the virtual Machine
#' @param storageAccount Name of the azure storage account
#' @param storageKey Storage key associated with storage account
#' @param blob Blob name
#' @param clustername Cluster name, used for HDI and Spark clusters. See \code{\link{azureCreateHDI}}
#' @param sessionID Spark sessionID. See \code{\link{azureSparkCMD}}
#' @param hdiAdmin HDInsight admin username
#' @param hdiPassword  HDInsight admin password
#' @param container Storage container name. See \code{\link{azureListStorageContainers}}
#' @param kind HDinsight kind: "hadoop","spark" or "pyspark"
#'
#' @family azureActiveContext functions
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
