azureApiHeaders <- function(token) {
  headers <- c(Host = "management.azure.com",
               Authorization = token,
                `Content-type` = "application/json")
  httr::add_headers(.headers = headers)
}

# convert verbose=TRUE to httr verbose
set_verbosity <- function(verbose = FALSE) {
  if (verbose) httr::verbose(TRUE) else NULL
}

extractUrlArguments <- function(x) {
  ptn <- ".*\\?(.*?)"
  args <- grepl("\\?", x)
  z <- if (args) gsub(ptn, "\\1", x) else ""
  if (z == "") {
    ""
  } else {
    z <- strsplit(z, "&")[[1]]
    z <- sort(z)
    z <- paste(z, collapse = "\n")
    z <- gsub("=", ":", z)
    paste0("\n", z)
  }
}

callAzureStorageApi <- function(url, verb = "GET", storageKey, storageAccount,
                   headers = NULL, container = NULL, CMD, size = nchar(content), contenttype = NULL,
                   content = NULL,
                   verbose = FALSE) {
  dateStamp <- httr::http_date(Sys.time())

  verbosity <- set_verbosity(verbose) 

  if (missing(CMD) || is.null(CMD)) CMD <- extractUrlArguments(url)

    sig <- createAzureStorageSignature(url = url, verb = verb,
      key = storageKey, storageAccount = storageAccount, container = container,
      headers = headers, CMD = CMD, size = size,
      contenttype = contenttype, dateStamp = dateStamp, verbose = verbose)

  azToken <- paste0("SharedKey ", storageAccount, ":", sig)

  switch(verb, 
  "GET" = GET(url, add_headers(.headers = c(Authorization = azToken,
                                    `Content-Length` = "0",
                                    `x-ms-version` = "2015-04-05",
                                    `x-ms-date` = dateStamp)
                                    ),
    verbosity),
  "PUT" = PUT(url, add_headers(.headers = c(Authorization = azToken,
                                         `Content-Length` = nchar(content),
                                         `x-ms-version` = "2015-04-05",
                                         `x-ms-date` = dateStamp,
                                         `x-ms-blob-type` = "Blockblob",
                                         `Content-type` = "text/plain; charset=UTF-8")),
           body = content,
    verbosity)
  )
}


createAzureStorageSignature <- function(url, verb, 
  key, storageAccount, container = NULL,
  headers = NULL, CMD = NULL, size = NULL, contenttype = NULL, dateStamp, verbose = FALSE) {

  if (missing(dateStamp)) {
    dateStamp <- httr::http_date(Sys.time())
  }

  arg1 <- if (length(headers)) {
    paste0(headers, "\nx-ms-date:", dateStamp, "\nx-ms-version:2015-04-05")
  } else {
    paste0("x-ms-date:", dateStamp, "\nx-ms-version:2015-04-05")
  }

  arg2 <- paste0("/", storageAccount, "/", container, CMD)

  SIG <- paste0(verb, "\n\n\n", size, "\n\n", contenttype, "\n\n\n\n\n\n\n",
                   arg1, "\n", arg2)
  if (verbose) message(paste0("TRACE: STRINGTOSIGN: ", SIG))
  base64encode(hmac(key = base64decode(key),
                    object = iconv(SIG, "ASCII", to = "UTF-8"),
                    algo = "sha256",
                    raw = TRUE)
                   )
}


#x_ms_date <- function() {
  #english <- "English_United Kingdom.1252"
  #old_locale <- Sys.getlocale(category = "LC_TIME")
  #on.exit(Sys.setlocale(locale = old_locale))
  #Sys.setlocale(category = "LC_TIME", locale = english)
  #strftime(Sys.time(), "%a, %d %b %Y %H:%M:%S %Z", tz = "GMT")
#}

x_ms_date <- function() httr::http_date(Sys.time())

azure_storage_header <- function(shared_key, date = x_ms_date(), content_length = 0) {
  if(!is.character(shared_key)) stop("Expecting a character for `shared_key`")
  headers <- c(
      Authorization = shared_key,
      `Content-Length` = as.character(content_length),
      `x-ms-version` = "2015-04-05",
      `x-ms-date` = date
  )
  add_headers(.headers = headers)
}

getSig <- function(azureActiveContext, url, verb, key, storageAccount,
                   headers = NULL, container = NULL, CMD = NULL, size = NULL, contenttype = NULL,
                   date = x_ms_date(), verbose = FALSE) {

  arg1 <- if (length(headers)) {
    paste0(headers, "\nx-ms-date:", date, "\nx-ms-version:2015-04-05")
  } else {
    paste0("x-ms-date:", date, "\nx-ms-version:2015-04-05")
  }

  arg2 <- paste0("/", storageAccount, "/", container, CMD)

  SIG <- paste0(verb, "\n\n\n", size, "\n\n", contenttype, "\n\n\n\n\n\n\n",
                   arg1, "\n", arg2)
  if (verbose) message(paste0("TRACE: STRINGTOSIGN: ", SIG))
  base64encode(hmac(key = base64decode(key),
                    object = iconv(SIG, "ASCII", to = "UTF-8"),
                    algo = "sha256",
                    raw = TRUE)
                   )
  }


stopWithAzureError <- function(r) {
  #if (status_code(r) %in% c(200, 201, 202, 204)) return()
  #browser()
  if(status_code(r) < 300) return()
  msg <- paste0(as.character(sys.call(1))[1], "()") # Name of calling fucntion
  addToMsg <- function(x) {
    if (!is.null(x)) x <- strwrap(x)
    if(is.null(x)) msg else c(msg, x)
    }
  if(inherits(content(r), "xml_document")){
    rr <- XML::xmlToList(XML::xmlParse(content(r)))
    msg <- addToMsg(rr$Code)
    msg <- addToMsg(rr$Message)
  } else {
    rr <- content(r)
    msg <- addToMsg(rr$code)
    msg <- addToMsg(rr$message)
    msg <- addToMsg(rr$error$message)
  }
  msg <- addToMsg(paste0("Return code: ", status_code(r)))
  msg <- paste(msg, collapse = "\n")
  stop(msg, call. = FALSE)
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


updateAzureActiveContext <- function(x, storageAccount, storageKey, resourceGroup, container, blob, directory) {
  # updates the active azure context in place
  if (!is.azureActiveContext(x)) return(FALSE)
  if (!missing(storageAccount)) x$storageAccount <- storageAccount
  if (!missing(resourceGroup))  x$resourceGroup  <- resourceGroup
  if (!missing(storageKey))     x$storageKey     <- storageKey
  if (!missing(container)) x$container <- container
  if (!missing(blob)) x$blob <- blob
  if (!missing(directory)) x$directory <- directory
  TRUE
}

validateStorageArguments <- function(resourceGroup, storageAccount, container, storageKey) {
  msg <- character(0)
  pasten <- function(x, ...) paste(x, ..., collapse = "", sep = "\n")
  if (!missing(resourceGroup) && (is.null(resourceGroup) || length(resourceGroup) == 0)) {
    msg <- pasten(msg, "- No resourceGroup provided. Use resourceGroup argument or set in AzureContext")
  }
  if (!missing(storageAccount) && (is.null(storageAccount) || length(storageAccount) == 0)) {
    msg <- pasten(msg, "- No storageAccount provided. Use storageAccount argument or set in AzureContext")
  }
  if (!missing(container) && (is.null(container) || length(container) == 0)) {
    msg <- pasten(msg, "- No container provided. Use container argument or set in AzureContext")
  }
  if (!missing(storageKey) && (is.null(storageKey) || length(storageKey) == 0)) {
    msg <- pasten(msg, "- No storageKey provided. Use storageKey argument or set in AzureContext")
  }

  if (length(msg) > 0) {
    stop(msg, call. = FALSE)
  }
  msg
}
