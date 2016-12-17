
#'
#' Create an AzureContext.
#'
#' @family Context Object
#' @export
CreateAzureContext <- function(){
  AzEnv <- new.env(parent = globalenv())
  AzEnv$TID <- "xxx"
  AzEnv$CID <- "xxx"
  AzEnv$KEY <- "xxx"
  return(AzEnv)
  return(1)
}


#' Dumps the contents of the AzureContext.
#'
#' @inheritParams SetAzureContext
#' @family Context Object
#' @export

DumpAzureContext <- function(AzureActiveContext){
  ls.str(envir = AzureActiveContext)
}


#' Updates the value of an AzureContext variable.
#'
#' @param AzureActiveContext Context Object
#' @param TID Tenant ID Object
#' @param CID Client ID Object
#' @param KEY Authentication KEY Object
#' @param SubscriptionID SubscriptionID Object
#' @param AzToken Token Object
#' @param ResourceGroup ResourceGroup Object
#' @param VMName VMName Object
#' @param StorageAccount StorageAccount Object
#' @param StorageKey StorageKey Object
#' @param Blob Blob Object
#' @param ClusterName ClusterName Object
#' @param SessionID SessionID Object
#' @param HDIAdmin HDIAdmin Object
#' @param HDIPassword HDIPassword Object
#' @param Container Container Object
#' @param Kind "hadoop","spark" or "pyspark"
#
# @param Log Log Object#'
#'
#' @export
SetAzureContext <- function(AzureActiveContext,TID, CID, KEY,AzToken,
                            SubscriptionID,ResourceGroup,
                            StorageKey,StorageAccount,
                            Container,Blob,
                            VMName,
                            HDIAdmin,HDIPassword,ClusterName,Kind,SessionID)
{
  if (!missing(TID)) AzureActiveContext$TID <- TID
  if (!missing(CID)) AzureActiveContext$CID <- CID
  if (!missing(KEY)) AzureActiveContext$KEY <- KEY
  if (!missing(AzToken)) AzureActiveContext$AZtoken <- AzToken
  if (!missing(SubscriptionID)) AzureActiveContext$SubscriptionID <- SubscriptionID
  if (!missing(ResourceGroup)) AzureActiveContext$ResourceGroup <- ResourceGroup
  if (!missing(StorageKey)) AzureActiveContext$StorageKey <- StorageKey
  if (!missing(StorageAccount)) AzureActiveContext$StorageAccount <- StorageAccount
  if (!missing(Container)) AzureActiveContext$Container <- Container
  if (!missing(Blob)) AzureActiveContext$Container <- Blob
  if (!missing(VMName)) AzureActiveContext$VMName <- VMName
  if (!missing(ClusterName)) AzureActiveContext$ClusterName <- ClusterName
  if (!missing(HDIAdmin)) AzureActiveContext$HDIAdmin <- HDIAdmin
  if (!missing(HDIPassword)) AzureActiveContext$HDIPassword <- HDIPassword
  if (!missing(Kind)) AzureActiveContext$Kind <- Kind
  if (!missing(SessionID)) AzureActiveContext$SessionID <- SessionID
}
