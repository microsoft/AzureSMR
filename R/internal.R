stopWithAzureError <- function(r){
  if(inherits(content(r), "xml_document")){
    rr <- XML::xmlToList(XML::xmlParse(content(r)))
    if(!is.null(rr$Code)) message(rr$Code)
    if(!is.null(rr$Message)) message(rr$Message)
  } else {
    if(!is.null(r$error$code)) message(content(r)$error$code)
    if(!is.null(r$error$message)) message(content(r)$error$message)
  }
  stop("Error return code: ", status_code(r), call. = FALSE)
}

extractResourceGroupname <- function(x) gsub(".*?/resourceGroups/(.*?)(/.*)*$",  "\\1", x)
extractSubscriptionID    <- function(x) gsub(".*?/subscriptions/(.*?)(/.*)*$",   "\\1", x)
extractStorageAccount    <- function(x) gsub(".*?/storageAccounts/(.*?)(/.*)*$", "\\1", x)

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
