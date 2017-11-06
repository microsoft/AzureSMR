# AzureSMR: Manage and Interact with Azure Resources.

[![Project Status: Active - The project has reached a stable, usable state and is being actively developed.](http://www.repostatus.org/badges/latest/active.svg)](http://www.repostatus.org/#active)
 
---
 
 `AzureSMR` is an R Package for managing a selection of Azure resources, using the Azure Service Manager API. The package exposes function to manage resources, resource groups, storage (blobs and containers), ARM templates, virtual machines, HDInsight (nodes, Hive and Spark) and Azure Datalake Store. To use the package, you must configure an Azure Active Directory application and service principal in the Azure portal.

To get started with this package, see the vignettes:

  * [Tutorial](http://htmlpreview.github.io/?https://github.com/Microsoft/AzureSMR/blob/master/inst/doc/tutorial.html)
  * [Getting Authenticated](http://htmlpreview.github.io/?https://github.com/Microsoft/AzureSMR/blob/master/inst/doc/Authentication.html)

To access the package help, just type `?AzureSMR` into your code editor.

Technical note: The package connects to Azure using standard CRAN packages (for example `httr` and `jsonlite`). This means you can use open source R to connect to Azure - you don't need Microsoft R Server.

## Code of conduct

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).  
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.
