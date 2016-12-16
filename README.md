# AzureSMR

R Package for managing a selection of Azure resources. Targeted at Data Scientists who need to control Azure Resources within an R session without needing to bother system administrators. 

APIs include Storage Blobs, HDInsight(Nodes, Hive, Spark), ARM, VMs.

To get started with this package, see the Vignettes:

  * [Tutorial](https://github.com/Microsoft/AzureSMR/blob/master/vignettes/tutorial.Rmd)
  * [Getting Authenticated](https://github.com/Microsoft/AzureSMR/blob/master/vignettes/Authentication.Rmd)

There is also help pages within the package that can be accessed from IDEs like RStudio. Just type AzureSM into search when the package is loaded to see a list of functions/help pages.

Note: The package imports standard R packages including `httr` and `jsonlite`. This means it can run in any open source R Session. 
