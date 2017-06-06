\dontrun{
library(AzureSMR)

azureCreateHDI(asc, resourceGroup = resourceGroup_name, clustername = "myazuresmrhdiclust",
               storageAccount = "azuresmrhdisa123",
               adminUser = "hdiadmin", adminPassword = "Password123!",
               sshUser = "sshuser", sshPassword = "Password123!",
               kind = "rserver", version = "3.5"
)
}