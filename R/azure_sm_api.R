# Define Azure API function
#azure_sm_url <- function(
  #resource_group,
  #subscription_id,
  #uri = "https://management.azure.com/",
  #api_version = "2015-01-01"
  #) {
  
  #paste0(uri, 
    #"subscriptions",
    #if (!missing(subscription_id)) paste0("/", subscription_id),
    #if (!missing(resource_group) && !is.null(resource_group)) {
      #paste0("/resourcegroups", if (resource_group != "") paste0("/", resource_group))
    #},
    #"?", "api-version=", api_version
    #)

#}

call_azure_sm <- function(azureActiveContext,
                          uri,
                          body = NULL,
                          verb = c("GET", "DELETE", "PUT", "POST"),
                          verbose = FALSE
  ) {

  assert_that(is.azureActiveContext(azureActiveContext))
  azToken <- azureActiveContext$Token
  verbosity <- set_verbosity(verbose)

  # define httr verb
  verb = match.arg(verb)
  verb <- switch(verb,
    GET = httr::GET,
    PUT = httr::PUT,
    DELETE = httr::DELETE,
    POST = httr::POST
    )

  r <- verb(uri, azureApiHeaders(azToken), body = body, verbosity, encode = "json")
  
}