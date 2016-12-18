stopWithAzureError <- function(r){
  message(content(r)$error$code)
  message(content(r)$error$message)
  stop("Error return code: ", status_code(r), call. = FALSE)
}
