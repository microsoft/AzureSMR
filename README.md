# AzureSMR
R Package for managing a selection of Azure resources. Targeted at Data Scientists who need to control Azure Resources within an R session without needing to bother system administrators. 

APIs include Storage Blobs, HDInsight(Nodes, Hive, Spark), ARM, VMs.

To understand more on how to use the APIs please check out the Vigntess

  [Tutorial](https://github.com/Microsoft/AzureSMR/blob/master/vignettes/tutorial.Rmd)
  
  [Getting Authenticated](https://github.com/Microsoft/AzureSMR/blob/master/vignettes/Authentication.Rmd)

There is also help pages within the package that can be accessed from IDEs like RStudio. Just type AzureSM into search when the package is loaded to see a list of functions/help pages.

Note: The package leverages standard R packages such as HTTR JSONLITE. So it can run in any  RSession. 
