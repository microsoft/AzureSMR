## ---- eval = FALSE-------------------------------------------------------
#  sc <- createAzureContext(tenantID = "{TID}", clientID = "{CID}", authKey= "{KEY}")
#  rgs <- azureListRG(sc)
#  rgs

## ---- eval = FALSE-------------------------------------------------------
#  sc <- createAzureContext(tenantID = "{TID}", clientID = "{CID}", authType= "DeviceCode")
#  # Manually authenicate using DeviceCode flow
#  rgs <- azureListRG(sc)
#  rgs

