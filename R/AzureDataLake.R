
#' Azure Data Lake LISTSTATUS for specified relativePath of an azure data lake account.
#'
#' @inheritParams createAzureContext
#' @inheritParams azureAuthenticate
#'
#' @param azureActiveContext Provide an `azureActiveContext` object used for authentication.
#' @param azureDataLakeAccount Provide the name of the Azure Data Lake account.
#' @param relativePath Provide a relative path of the directory.
#' @param verbose Print tracing information (default FALSE).
#'
#' @return Returns a data frame.
#'
#' @template
#' @references
#' @family Azure Data Lake Store functions
#' @export
azureDataLakeListStatus <- function(azureActiveContext, azureDataLakeAccount, relativePath = "", verbose = FALSE) {

  if (!missing(azureActiveContext) && !is.null(azureActiveContext)) {
    assert_that(is.azureActiveContext(azureActiveContext))
    azureCheckToken(azureActiveContext)
  }

  # ToDo: Need to check if ADLS requires a different one
  assert_that(is_storage_account(azureDataLakeAccount))

  verbosity <- set_verbosity(verbose)

  URL <- paste0(
    "https://", azureDataLakeAccount, ".azuredatalakestore.net/webhdfs/v1/",
    relativePath,
    "?op=LISTSTATUS",
    "&api-version=2016-11-01"
    )

  resHttp <- callAzureDataLakeApi(URL,
    azureActiveContext = azureActiveContext,
    verbose = verbose)

  if (status_code(resHttp) == 404) {
    warning("Azure data lake response: resource not found")
    return(NULL)
  }
  stopWithAzureError(resHttp)

  resJsonStr <- content(resHttp, "text", encoding = "UTF-8")
  if (length(resJsonStr) == 0) {
    return(
      as.data.frame()
    )
  }
  resJson <- jsonlite::fromJSON(resJsonStr)
  resDf <- as.data.frame(resJson)
  resDf
}

