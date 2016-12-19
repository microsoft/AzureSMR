stopWithAzureError <- function(r){
  message(content(r)$error$code)
  message(content(r)$error$message)
  stop("Error return code: ", status_code(r), call. = FALSE)
}

extractResourceGroupName <- function(x) gsub(".*?/resourceGroups/(.*?)(/.*)*$", "\\1", x)
extractSubscriptionID    <- function(x) gsub(".*?/subscriptions/(.*?)(/.*)*$",  "\\1", x)

refreshStorageKey <- function(AzureActiveContext){
  if (length(AzureActiveContext$StorageAccountK) < 1 ||
      SAI != AzureActiveContext$StorageAccountK ||
      length(AzureActiveContext$StorageKey) <1
  ) {
    message("Fetching Storage Key..")
    AzureSAGetKey(AzureActiveContext, ResourceGroup = RGI, StorageAccount = SAI)
  } else {
    AzureActiveContext$StorageKey
  }
}
