\dontrun{
library(AzureSMR)

azureCreateHDI(asc, resourceGroup = resourceGroup_name, clustername = "azuresmr_hdi_test",
      storageAccount = "azuresmrhditestsa",
      adminUser = "hdiadmin", adminPassword = "Azuresmr_password1",
      sshUser = "sssUser_test1", sshPassword = "sshUser_password",
      kind = "rserver",
      debug = TRUE
)
}
