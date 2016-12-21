stopWithAzureError <- function(r){
  if(!is.null(r$error$code))
    message(content(r)$error$code)
  if(!is.null(r$error$message))
    message(content(r)$error$message)
  stop("Error return code: ", status_code(r), call. = FALSE)
}

extractResourceGroupname <- function(x) gsub(".*?/resourceGroups/(.*?)(/.*)*$", "\\1", x)
extractSubscriptionID    <- function(x) gsub(".*?/subscriptions/(.*?)(/.*)*$",  "\\1", x)

refreshStorageKey <- function(azureActiveContext, storageAccount, resourceGroup){
  if (length(azureActiveContext$storageAccountK) < 1 ||
      storageAccount != azureActiveContext$storageAccountK ||
      length(azureActiveContext$storageKey) <1
  ) {
    message("Fetching Storage Key..")
    azureSAGetKey(azureActiveContext, resourceGroup = resourceGroup, storageAccount = storageAccount)
  } else {
    azureActiveContext$storageKey
  }
}
