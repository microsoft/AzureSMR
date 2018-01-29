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
                   headers = NULL, container = NULL, CMD, size = getContentSize(content), contenttype = NULL,
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
                                    `x-ms-version` = "2017-04-17",
                                    `x-ms-date` = dateStamp)
                                    ),
    verbosity),
  "PUT" = PUT(url, add_headers(.headers = c(Authorization = azToken,
                                         `Content-Length` = size,
                                         `x-ms-version` = "2017-04-17",
                                         `x-ms-date` = dateStamp,
                                         `x-ms-blob-type` = "Blockblob",
                                         `Content-type` = contenttype)),
           body = content,
    verbosity)
  )
}

getContentSize<- function(obj) {
    switch(class(obj),
         "raw" = length(obj),
         "character" = nchar(obj),
         nchar(obj))
}

createAzureStorageSignature <- function(url, verb, 
  key, storageAccount, container = NULL,
  headers = NULL, CMD = NULL, size = NULL, contenttype = NULL, dateStamp, verbose = FALSE) {

  if (missing(dateStamp)) {
    dateStamp <- httr::http_date(Sys.time())
  }

  arg1 <- if (length(headers)) {
    paste0(headers, "\nx-ms-date:", dateStamp, "\nx-ms-version:2017-04-17")
  } else {
    paste0("x-ms-date:", dateStamp, "\nx-ms-version:2017-04-17")
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

x_ms_date <- function() httr::http_date(Sys.time())

azure_storage_header <- function(shared_key, date = x_ms_date(), content_length = 0) {
  if(!is.character(shared_key)) stop("Expecting a character for `shared_key`")
  headers <- c(
      Authorization = shared_key,
      `Content-Length` = as.character(content_length),
      `x-ms-version` = "2017-04-17",
      `x-ms-date` = date
  )
  add_headers(.headers = headers)
}

getAzureDataLakeSDKVersion <- function() {
  return("1.0.0.0")
}

getAzureDataLakeSDKUserAgent <- function() {
  adlsUA <- paste0("ADLSRSDK"
                   , "-", getAzureDataLakeSDKVersion()
                   , "/", Sys.info()[1], "-", Sys.info()[2] #sysname-release
                   , "-", Sys.info()[3] #version
                   , "-", Sys.info()[5] #machine
                   , "/", R.version$version.string
  )
  return(adlsUA)
}

getAzureDataLakeBasePath <- function(azureDataLakeAccount) {
  basePath <- paste0("https://", azureDataLakeAccount, ".azuredatalakestore.net/webhdfs/v1/")
  return(basePath)
}

getAzureDataLakeApiVersion <- function() {
  return("&api-version=2016-11-01")
}

callAzureDataLakeApi <- function(url, verb = "GET", azureActiveContext,
                                content = raw(0), contenttype = "text/plain; charset=UTF-8",
                                verbose = FALSE) {
  verbosity <- set_verbosity(verbose)
  commonHeaders <- c(Authorization = azureActiveContext$Token
                     , `User-Agent` = getAzureDataLakeSDKUserAgent()
                     , `x-ms-client-request-id` = uuid()
                     )
  resHttp <- switch(verb,
         "GET" = GET(url,
                     add_headers(.headers = c(commonHeaders
                                              , `Content-Length` = "0"
                                              )
                                 ),
                     verbosity
                     ),
         "PUT" = PUT(url,
                     add_headers(.headers = c(commonHeaders
                                              , `Transfer-Encoding` = "chunked"
                                              , `Content-Length` = getContentSize(content)
                                              , `Content-type` = contenttype
                                              )
                                 ),
                     body = content,
                     verbosity
                     ),
         "POST" = POST(url,
                     add_headers(.headers = c(commonHeaders
                                              , `Transfer-Encoding` = "chunked"
                                              , `Content-Length` = getContentSize(content)
                                              , `Content-type` = contenttype
                                              )
                                 ),
                     body = content,
                     verbosity
                     ),
         "DELETE" = DELETE(url,
                     add_headers(.headers = c(commonHeaders
                                              , `Content-Length` = "0"
                                              )
                                 ),
                     verbosity
                     )
         )
  # Print the response body in case verbose is enabled.
  if (verbose) {
    resJsonStr <- content(resHttp, "text", encoding = "UTF-8")
    print(resJsonStr)
  }
  return(resHttp)
}

getSig <- function(azureActiveContext, url, verb, key, storageAccount,
                   headers = NULL, container = NULL, CMD = NULL, size = NULL, contenttype = NULL,
                   date = x_ms_date(), verbose = FALSE) {

  arg1 <- if (length(headers)) {
    paste0(headers, "\nx-ms-date:", date, "\nx-ms-version:2017-04-17")
  } else {
    paste0("x-ms-date:", date, "\nx-ms-version:2017-04-17")
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
  if (status_code(r) < 300) return()
  msg <- paste0(as.character(sys.call(1))[1], "()") # Name of calling fucntion
  addToMsg <- function(x) {
    if (!is.null(x)) x <- strwrap(x)
    if(is.null(x)) msg else c(msg, x)
    }
  if(inherits(content(r), "xml_document")){
    rr <- XML::xmlToList(XML::xmlParse(content(r)))
    msg <- addToMsg(rr$Code)
    msg <- addToMsg(rr$Message)
    msg <- addToMsg(rr$AuthenticationErrorDetail)
  } else {
    rr <- content(r)
    msg <- addToMsg(rr$code)
    msg <- addToMsg(rr$message)
    msg <- addToMsg(rr$error$message)
    
    msg <- addToMsg(rr$Code)
    msg <- addToMsg(rr$Message)
    msg <- addToMsg(rr$Error$Message)
    
  }
  msg <- addToMsg(paste0("Return code: ", status_code(r)))
  msg <- paste(msg, collapse = "\n")
  stop(msg, call. = FALSE)
}

extractResourceGroupname <- function(x) gsub(".*?/resourceGroups/(.*?)(/.*)*$",  "\\1", x)
extractSubscriptionID    <- function(x) gsub(".*?/subscriptions/(.*?)(/.*)*$",   "\\1", x)
extractStorageAccount    <- function(x) gsub(".*?/storageAccounts/(.*?)(/.*)*$", "\\1", x)


refreshStorageKey <- function(azureActiveContext, storageAccount, resourceGroup){
  if (storageAccount != azureActiveContext$storageAccount ||
      length(azureActiveContext$storageKey) == 0
  ) {
    message("Fetching Storage Key..")
    azureSAGetKey(azureActiveContext, resourceGroup = resourceGroup, storageAccount = storageAccount)
  } else {
    azureActiveContext$storageKey
  }
}


updateAzureActiveContext <- function(x, storageAccount, storageKey, resourceGroup, container, blob, directory) {
  # updates the active azure context in place
  if (!is.null(x)) {
    assert_that(is.azureActiveContext(x))
    if (!missing(storageAccount)) x$storageAccount <- storageAccount
    if (!missing(resourceGroup))  x$resourceGroup  <- resourceGroup
    if (!missing(storageKey))     x$storageKey     <- storageKey
    if (!missing(container)) x$container <- container
    if (!missing(blob)) x$blob <- blob
    if (!missing(directory)) x$directory <- directory
  }
  TRUE
}

## https://gist.github.com/cbare/5979354
## Version 4 UUIDs have the form xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx
## where x is any hexadecimal digit and y is one of 8, 9, A, or B
## e.g., f47ac10b-58cc-4372-a567-0e02b2c3d479
uuid <- function(uppercase=FALSE) {
  
  hex_digits <- c(as.character(0:9), letters[1:6])
  hex_digits <- if (uppercase) toupper(hex_digits) else hex_digits
  
  y_digits <- hex_digits[9:12]
  
  paste(
    paste0(sample(hex_digits, 8, replace=TRUE), collapse=''),
    paste0(sample(hex_digits, 4, replace=TRUE), collapse=''),
    paste0('4', paste0(sample(hex_digits, 3, replace=TRUE), collapse=''), collapse=''),
    paste0(sample(y_digits,1), paste0(sample(hex_digits, 3, replace=TRUE), collapse=''), collapse=''),
    paste0(sample(hex_digits, 12, replace=TRUE), collapse=''),
    sep='-')
}
