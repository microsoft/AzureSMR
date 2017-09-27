#' Create an Azure Context.
#'
#' Create a container (`azureActiveContext`) for holding variables used by the `AzureSMR` package.  If `tenantID`, `clientID` and `authKey` are provided, attempts to authenticate the session against Azure.
#'
#' See the Azure documentation (\url{https://azure.microsoft.com/en-us/documentation/articles/resource-group-create-service-principal-portal/}) for information to configure an Active Directory application.
#'
#' @inheritParams setAzureContext
#' @inheritParams read.AzureSMR.config
#' @family azureActiveContext functions
#'
#' @seealso [setAzureContext()], [azureAuthenticate()], [read.AzureSMR.config)]
#' @return An `azureActiveContext` object
#' @export
createAzureContext <- function(tenantID, clientID, authKey, configFile){
  azEnv <- new.env(parent = emptyenv())
  azEnv <- as.azureActiveContext(azEnv)

  list2env(
    list(tenantID = "", clientID = "", authKey = ""),
    envir = azEnv
  )
  if (!missing(configFile)) {
    config <- read.AzureSMR.config(configFile)
    list2env(config, envir = azEnv)
    azureAuthenticate(azEnv)
  } else {
    if (!missing(tenantID)) azEnv$tenantID <- tenantID
    if (!missing(clientID)) azEnv$clientID <- clientID
    if (!missing(authKey)) azEnv$authKey <- authKey
    if (!missing(tenantID) && !missing(clientID) && !missing(authKey)) {
      azureAuthenticate(azEnv)
    }
  }

  return(azEnv)
}

#' Updates azureActiveContext object.
#'
#' Updates the value of an `azureActiveContext` object, created by [createAzureContext()]
#'
#' @param azureActiveContext A container used for caching variables used by `AzureSMR`, created by [createAzureContext()]
#' @param tenantID The tenant ID provided during creation of the Active Directory application / service principal
#' @param clientID The client ID provided during creation of the Active Directory application / service principal
#' @param authKey The authentication key provided during creation of the Active Directory application / service principal
#' @param subscriptionID Subscription ID.  This is obtained automatically by [azureAuthenticate()] when only a single subscriptionID is available via Active Directory
#' @param resourceGroup Name of the resource group
#' @param vmName Name of the virtual machine
#' @param storageAccount Name of the azure storage account. Storage account names must be between 3 and 24 characters in length and use numbers and lower-case letters only.
#' @param storageKey Storage key associated with storage account
#' @param blob Blob name
#' @param clustername Cluster name, used for HDI and Spark clusters. See [azureCreateHDI()]
#' @param sessionID Spark sessionID. See [azureSparkCMD()]
#' @param hdiAdmin HDInsight admin username. See [azureCreateHDI()]
#' @param hdiPassword  HDInsight admin password. See [azureCreateHDI()]
#' @param container Storage container name. See [azureListStorageContainers()]
#' @param kind HDinsight kind: "hadoop","spark" or "rserver". See [azureCreateHDI()]
#'
#' @family azureActiveContext functions
#' @export
setAzureContext <- function(azureActiveContext, tenantID, clientID, authKey, 
                            subscriptionID, resourceGroup,
                            storageKey, storageAccount,
                            container, blob,
                            vmName,
                            hdiAdmin, hdiPassword, clustername, kind, sessionID)
{
  if (!missing(tenantID)) azureActiveContext$tenantID <- tenantID
  if (!missing(clientID)) azureActiveContext$clientID <- clientID
  if (!missing(authKey)) azureActiveContext$authKey <- authKey

  if (!missing(subscriptionID)) {
    assert_that(is_subscription_id(subscriptionID))
    azureActiveContext$subscriptionID <- subscriptionID
  }
  if (!missing(resourceGroup)) {
    assert_that(is_resource_group(resourceGroup))
    azureActiveContext$resourceGroup <- resourceGroup
  }
  if (!missing(storageKey)) {
    assert_that(is_storage_key(storageKey))
    azureActiveContext$storageKey <- storageKey
  }
  if (!missing(storageAccount)) {
    assert_that(is_storage_account(storageAccount))
    azureActiveContext$storageAccount <- storageAccount
  }
  if (!missing(container)) {
    assert_that(is_container(container))
    azureActiveContext$container <- container
  }
  if (!missing(blob)) {
    assert_that(is_blob(blob))
    azureActiveContext$container <- blob
  }

  if (!missing(vmName)) {
    assert_that(is_vm_name(vmName))
    azureActiveContext$vmName <- vmName
  }

  if (!missing(clustername)) azureActiveContext$clustername <- clustername
  if (!missing(hdiAdmin)) azureActiveContext$hdiAdmin <- hdiAdmin
  if (!missing(hdiPassword)) azureActiveContext$hdiPassword  <- hdiPassword
  if (!missing(kind)) azureActiveContext$kind <- kind
  if (!missing(sessionID)) azureActiveContext$sessionID <- sessionID
}
