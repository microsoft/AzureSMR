Sys.setenv("R_TESTS" = "")
library(testthat, quietly = TRUE)
if (identical(Sys.getenv("NOT_CRAN"), "true")) {
  # NOT_CRAN
  # run all tests
  
  test_check("AzureSMR")
  # test_check("AzureSMR", filter = "1-authentication")
  # test_check("AzureSMR", filter = "2-resources")
  #test_check("AzureSMR", filter = "7-datalake")

} else {
  # CRAN
  # skip some tests on CRAN, to comply with timing directive and other policy
  test_check("AzureSMR")
  # test_check("AzureSMR", filter = "1-authentication")
  # test_check("AzureSMR", filter = "2-resources")
  #test_check("AzureSMR", filter = "7-datalake")
}
