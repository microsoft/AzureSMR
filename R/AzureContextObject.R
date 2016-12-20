
#'
#' Create an AzureContext.
#'
#' @family Context Object
#' @export
createAzureContext <- function(){
  AzEnv <- new.env(parent = globalenv())
  AzEnv$TID <- "xxx"
  AzEnv$CID <- "xxx"
  AzEnv$KEY <- "xxx"
  return(AzEnv)
  return(1)
}


#' Dumps the contents of the AzureContext.
#'
#' @inheritParams setAzureContext
#' @family Context Object
#' @export

dumpAzureContext <- function(azureActiveContext){
  ls.str(envir = azureActiveContext)
}


#' Updates the value of an AzureContext variable.
#'
#' @param azureActiveContext Context Object
#' @param TID Tenant ID Object
#' @param CID Client ID Object
#' @param KEY Authentication KEY Object
#' @param subscriptionID subscriptionID Object
#' @param azToken Token Object
#' @param resourceGroup resourceGroup Object
#' @param vmName vmName Object
#' @param storageAccount storageAccount Object
#' @param storageKey storageKey Object
#' @param blob blob Object
#' @param clustername clustername Object
#' @param sessionID sessionID Object
#' @param hdiAdmin hdiAdmin Object
#' @param hdiPassword  hdiPassword  Object
#' @param container container Object
#' @param kind "hadoop","spark" or "pyspark"
#
# @param log log Object#'
#'
#' @export
setAzureContext <- function(azureActiveContext,TID, CID, KEY,azToken,
                            subscriptionID,resourceGroup,
                            storageKey,storageAccount,
                            container,blob,
                            vmName,
                            hdiAdmin,hdiPassword ,clustername,kind,sessionID)
{
  if (!missing(TID)) azureActiveContext$TID <- TID
  if (!missing(CID)) azureActiveContext$CID <- CID
  if (!missing(KEY)) azureActiveContext$KEY <- KEY
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
