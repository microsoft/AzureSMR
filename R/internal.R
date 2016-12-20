stopWithAzureError <- function(r){
  if(!is.null(r$error$code)) 
    message(content(r)$error$code)
  if(!is.null(r$error$message)) 
    message(content(r)$error$message)
  stop("Error return code: ", status_code(r), call. = FALSE)
}

extractresourceGroupname <- function(x) gsub(".*?/resourceGroups/(.*?)(/.*)*$", "\\1", x)
extractsubscriptionID    <- function(x) gsub(".*?/subscriptions/(.*?)(/.*)*$",  "\\1", x)

refreshstorageKey <- function(azureActiveContext){
  if (length(azureActiveContext$storageAccountK) < 1 ||
      SAI != azureActiveContext$storageAccountK ||
      length(azureActiveContext$storageKey) <1
  ) {
    message("Fetching Storage Key..")
    azureSAGetKey(azureActiveContext, resourceGroup = RGI, storageAccount = SAI)
  } else {
    azureActiveContext$storageKey
  }
}
