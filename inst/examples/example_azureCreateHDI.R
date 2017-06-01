\dontrun{
library(AzureSMR)

  azureCreateHDI(context, 
                 resourceGroup = RG, 
                 clustername = "smrhdi", # only low case letters, digit, and dash.
                 storageAccount = "smrhdisa",
                 adminUser = "hdiadmin", 
                 adminPassword = "AzureSMR_password123",
                 sshUser = "hdisshuser", 
                 sshPassword = "AzureSMR_password123", # need at least digits.
                 kind = "rserver", 
                 debug = FALSE
  )
      
}
