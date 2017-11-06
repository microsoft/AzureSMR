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
#'   - [azureGetBlob()]
#'   - [azurePutBlob()]
#'   - [azureDeleteBlob()]
#'   - [azureBlobFind()]
#' * Virtual Machines
#'   - List VMs: [azureListVM()]
#'   - To create a virtual machine, use [azureDeployTemplate()] with a suitable template
#'   - Start a VM: [azureStartVM()]
#'   - Stop a VM: [azureStopVM()]
#'   - Get status: [azureVMStatus()]
#' * HDInsight clusters:
#'   - [azureListHDI()]
#'   - [azureCreateHDI()]
#'   - [azureResizeHDI()]
#'   - [azureDeleteHDI()]
#'   - [azureRunScriptAction()]
#'   - [azureScriptActionHistory()]
#' * Azure batch:
#'   - [azureListBatchAccounts()]
#'   - [azureCreateBatchAccount()]
#'   - [azureDeleteBatchAccount()]
#'   - [azureBatchGetKey()]
#' * Cost functions:
#'   - [azureDataConsumption()]
#'   - [azurePricingRates()]
#'   - [azureExpenseCalculator()]
#' * Azure Data Lake Store functions:
#'   - [azureDataLakeListStatus()]
#'   - [azureDataLakeGetFileStatus()]
#'   - [azureDataLakeMkdirs()]
#'   - [azureDataLakeCreate()]
#'   - [azureDataLakeAppend()]
#'   - [azureDataLakeRead()]
#'   - [azureDataLakeDelete()]
#'
#' @name AzureSMR
#' @aliases AzureSMR-package
#' @docType package
#' @keywords package
#'
#' @importFrom utils browseURL URLencode ls.str str
#' @importFrom digest hmac
#' @importFrom base64enc base64encode base64decode
#' @importFrom jsonlite fromJSON
#' @importFrom httr add_headers headers content status_code http_status authenticate
#' @importFrom httr GET PUT DELETE POST
#' @importFrom XML htmlParse xpathApply xpathSApply xmlValue
#' @importFrom lubridate hour minute second
#'
NULL
