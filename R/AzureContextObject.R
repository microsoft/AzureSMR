
#' CreateAzureContext
#'
#' Create an AzureContext
#'
#' @title AzureSM: Create an AzureContext
#' @name CreateAzureContext
#' @rdname CreateAzureContext
#' @export
CreateAzureContext <- function(){
  AzEnv <- new.env(parent = globalenv())
  AzEnv$TID <- "xxx"
  AzEnv$CID <- "xxx"
  AzEnv$KEY <- "xxx"
  return(AzEnv)
  return(1)
}

#' DumpAzureContext
#'
#' Dumps the contents of the AzureContext
#'
#' @title AzureSM: Dumps the contents of the AzureContext
#' @param Azure Context Object
#' @name DumpAzureContext
#' @rdname DumpAzureContext
#' @export

DumpAzureContext <- function(AzEnv)
{
  ls.str(envir = AzEnv)
}

#' AzureSM: SetAzureContext
#'
#' Updates the value of an AzureContext variable
#' @title AzureSM: Updates the value of an AzureContext variable
#' @param Azure Context Object
#' @param TID Tenant ID Object
#' @param CID Client ID Object
#' @param KEY Authentication KEY Object
#' @param SubscriptionID SubscriptionID Object
#' @param Token Token Object
#' @param ResourceGroup ResourceGroup Object
#' @param VMName VMName Object
#' @param StorageAccount StorageAccount Object
#' @param StorageKey StorageKey Object
#' @param Blob Blob Object
#' @param ClusterName ClusterName Object
#' @param SessionID SessionID Object
#' @param HDIAdmin HDIAdmin Object
#' @param HDIPassword HDIPassword Object
#' @param Kind Kind Object
#' @param Container Container Object
#' @param Log Log Object#'
#' @author Alan Weaver
#' @name SetAzureContext
#' @rdname SetAzureContext
#' @export
SetAzureContext <- function(AzureActiveContext,TID, CID, KEY,Token, SubscriptionID,ResourceGroup,StorageKey,StorageAccount,Container,Blob,VMName,HDIAdmin,HDIPassword,ClusterName,Kind,SessionID)
{
  if (!missing(TID)) AzureActiveContext$TID <- TID
  if (!missing(CID)) AzureActiveContext$CID <- CID
  if (!missing(KEY)) AzureActiveContext$KEY <- KEY
  if (!missing(Token)) AzureActiveContext$AZtoken <- Token
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
