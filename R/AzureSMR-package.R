#' AzureSMR
#'
#' The AzureSMR package connects R to the Azure Service Manager API.
#'
#' This enables you to use and change many Azure resources. The following is an incomplete list of available functions:
#'
#' * Create an Azure context:
#'   - [createAzureContext()]
#' * Resources and resource groups:
#'   - [azureListAllResources()]
#'   - [azureCreateResourceGroup()]
#'   - [azureListRG()]
#' * Storage accounts and blobs:
#'   - [azureListSA()]
#'   - [azureCreateStorageAccount()]
#'   - [azureListStorageContainers()]
#'   - [azureListStorageBlobs()]
#' * HDInsight (Nodes, Hive, Spark)
#' * Azure Resource Manager
#' * Virtual Machines
#'
#'
#' @name AzureSMR-package
#' @aliases AzureSMR
#' @docType package
#' @keywords package
#'
#' @importFrom utils browseURL ls.str
#' @importFrom digest hmac
#' @importFrom base64enc base64encode base64decode
#' @importFrom plyr rbind.fill
#' @importFrom jsonlite fromJSON
#' @importFrom httr add_headers headers content status_code http_status authenticate
#' @importFrom httr GET PUT DELETE POST
#' @importFrom XML htmlParse xpathApply xpathSApply xmlValue
#'
NULL
