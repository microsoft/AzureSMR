getSig <- function(azureActiveContext, url, verb, key, storageAccount,
                   headers = NULL, container = NULL, CMD = NULL, size = NULL, contenttype = NULL,
                   dateS, verbose = FALSE) {

  if (length(headers)){
    ARG1 <- paste0(headers, "\nx-ms-date:", dateS, "\nx-ms-version:2015-04-05")
  } else {
    ARG1 <- paste0("x-ms-date:", dateS, "\nx-ms-version:2015-04-05")
  }

  ARG2 <- paste0("/", storageAccount, "/", container, CMD)

  SIG <- paste0(verb, "\n\n\n", size, "\n\n", contenttype, "\n\n\n\n\n\n\n",
                ARG1, "\n", ARG2)
  if (verbose) message(paste0("TRACE: STRINGTOSIGN: ", SIG))
  base64encode(hmac(key = base64decode(key),
                    object = iconv(SIG, "ASCII",to = "UTF-8"),
                    algo = "sha256",
                    raw = TRUE)
  )
}


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
