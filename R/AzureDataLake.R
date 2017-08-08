
#' Azure Data Lake LISTSTATUS for specified storage account.
#'
#' @inheritParams createAzureContext
#' @inheritParams azureAuthenticate
#'
#' @param azureActiveContext Provide an `azureActiveContext` object used for authentication.
#' @param azureDataLakeStoreAccount Provide the name of the Azure Data Lake Store account.
#'
#' @return Returns a data frame. This data frame has an attribute called `marker` that can be used with the `marker` argument to return the next set of values.
#'
#' @template
#' @references
#' @family Azure Data Lake Store functions
#' @export
azureDataLakeListStatus <- function(azureActiveContext, azureDataLakeStoreAccount, relativeFilePath = "", verbose = FALSE) {

  if (!missing(azureActiveContext) && !is.null(azureActiveContext)) {
    assert_that(is.azureActiveContext(azureActiveContext))
    azureCheckToken(azureActiveContext)
    if(missing(azureDataLakeStoreAccount)) azureDataLakeStoreAccount <- azureActiveContext$storageAccount
  }

  #assert_that(is_storage_account(storageAccount))

  verbosity <- set_verbosity(verbose)

  URL <- paste0("https://", azureDataLakeStoreAccount, ".azuredatalakestore.net/webhdfs/v1/", relativeFilePath, "?op=LISTSTATUS&api-version=2016-11-01")

  r <- callAzureDataLakeApi(URL,
    azureActiveContext = azureActiveContext,
    storageAccount = storageAccount,
    verbose = verbose)

  if (status_code(r) == 404) {
    warning("Azure data lake response: resource not found")
    return(NULL)
  }
  stopWithAzureError(r)

  jsonlite::toJSON(jsonlite::fromJSON(content(r, "text", encoding = "UTF-8")), pretty = TRUE)
}

