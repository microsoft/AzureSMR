## ---- eval=FALSE---------------------------------------------------------
#  # Install devtools
#  if(!require("devtools")) install.packages("devtools")
#  devtools::install_github("Microsoft/AzureSMR")
#  library(AzureSMR)

## ---- eval=FALSE---------------------------------------------------------
#  library(AzureSMR)

## ---- eval=FALSE---------------------------------------------------------
#  sc <- createAzureContext(tenantID = "{TID}", clientID = "{CID}", authKey= "{KEY}")
#  sc

## ---- eval = FALSE-------------------------------------------------------
#  sc <- createAzureContext(tenantID = "{TID}", clientID = "{CID}", authType= "DeviceCode")
#  # Manually authenticate using DeviceCode flow
#  rgs <- azureListRG(sc)
#  rgs

## ---- eval=FALSE---------------------------------------------------------
#  azureListSubscriptions(sc)

## ---- eval=FALSE---------------------------------------------------------
#  # list resource groups
#  azureListRG(sc)
#  
#  # list all resources
#  azureListAllResources(sc)
#  
#  azureListAllResources(sc, location = "northeurope")
#  
#  azureListAllResources(sc, type = "Microsoft.Sql/servers", location = "northeurope")
#  
#  azureCreateResourceGroup(sc, resourceGroup = "testme", location = "northeurope")
#  
#  azureCreateStorageAccount(sc,storageAccount="testmystorage1",resourceGroup = "testme")
#  
#  azureListAllResources(sc, resourceGroup = "testme")
#  
#  # When finished, to delete a Resource Group use azureDeleteResourceGroup()
#  azureDeleteResourceGroup(sc, resourceGroup = "testme")

## ---- eval=FALSE---------------------------------------------------------
#  ## List VMs in a ResourceGroup
#  azureListVM(sc, resourceGroup = "testme")
#  
#  ##            Name    Location                             Type    OS     State  Admin
#  ## 1         DSVM1 northeurope Microsoft.Compute/virtualMachines Linux Succeeded
#  
#  azureStartVM(sc, vmName = "DSVM1")
#  azureStopVM(sc, vmName = "DSVM1")

## ---- eval=FALSE---------------------------------------------------------
#  azureSAGetKey(sc, resourceGroup = "testme", storageAccount = "testmystorage1")

## ---- eval=FALSE---------------------------------------------------------
#  azureCreateStorageContainer(sc, "opendata", storageAccount = "testmystorage1", resourceGroup = "testme")

## ---- eval=FALSE---------------------------------------------------------
#  azureListStorageContainers(sc, storageAccount = "testmystorage1", resourceGroup = "testme")

## ---- eval=FALSE---------------------------------------------------------
#  azurePutBlob(sc, storageAccount = "testmystorage1", container = "opendata",
#               contents = "Hello World",
#               blob = "HELLO")

## ---- eval=FALSE---------------------------------------------------------
#  azureListStorageBlobs(sc, storageAccount = "testmystorage1", container = "opendata")

## ---- eval=FALSE---------------------------------------------------------
#  azureGetBlob(sc, storageAccount = "testmystorage1", container = "opendata",
#               blob="HELLO",
#               type="text")

## ---- eval=FALSE---------------------------------------------------------
#  azureListStorageBlobs(NULL, storageAccount = "testmystorage1", container = "opendata")

## ---- eval=FALSE---------------------------------------------------------
#  azureCreateHDI(sc,
#                   resourceGroup = "testme",
#                   clustername = "smrhdi", # only low case letters, digit, and dash.
#                   storageAccount = "testmystorage1",
#                   adminUser = "hdiadmin",
#                   adminPassword = "AzureSMR_password123",
#                   sshUser = "hdisshuser",
#                   sshPassword = "AzureSMR_password123",
#                   kind = "rserver")

## ---- eval=FALSE---------------------------------------------------------
#  azureListHDI(sc, resourceGroup ="testme")

## ---- eval=FALSE---------------------------------------------------------
#  azureResizeHDI(sc, resourceGroup = "testme", clustername = "smrhdi", role="workernode",size=3)
#  
#  ## azureResizeHDI: Request Submitted:  2016-06-23 18:50:57
#  ## Resizing(R), Succeeded(S)
#  ## RRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR
#  ## RRRRRRRRRRRRRRRRRRS
#  ## Finished Resizing Sucessfully:  2016-06-23 19:04:43
#  ## Finished:  2016-06-23 19:04:43
#  ##                                                                                                                        ## Information
#  ## " headnode ( 2 * Standard_D3_v2 ) workernode ( 5 * Standard_D3_v2 ) zookeepernode ( 3 * Medium ) edgenode0 ( 1 * Standard_D4_v2 )"

## ---- eval=FALSE---------------------------------------------------------
#  azureDeployTemplate(sc, resourceGroup = "Analytics", deplName = "Deploy1",
#                      templateURL = "{TEMPLATEURL}", paramURL = "{PARAMURL}")
#  
#  ## azureDeployTemplate: Request Submitted:  2016-06-23 18:50:57
#  ## Resizing(R), Succeeded(S)
#  ## RRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR
#  ## RRRRRRRRRRRRRRRRRRS
#  ## Finished Deployed Sucessfully:  2016-06-23 19:04:43
#  ## Finished:  2016-06-23 19:04:43

## ---- eval=FALSE---------------------------------------------------------
#  azureHiveStatus(sc, clusterName = "smrhdi",
#                  hdiAdmin = "hdiadmin",
#                  hdiPassword = "AzureSMR_password123")
#  
#  azureHiveSQL(sc,
#               CMD = "select * from hivesampletable",
#               path = "wasb://opendata@testmystorage1.blob.core.windows.net/")

## ---- eval=FALSE---------------------------------------------------------
#  azureSparkNewSession(sc, clustername = "smrhdi",
#                       hdiAdmin = "hdiadmin",
#                       hdiPassword = "AzureSMR_password123",
#                       kind = "pyspark")

## ---- eval=FALSE---------------------------------------------------------
#  azureSparkListSessions(sc, clustername = "smrhdi")

## ---- eval=FALSE---------------------------------------------------------
#  # SAMPLE PYSPARK SCRIPT TO CALCULATE PI
#  pythonCmd <- '
#  from pyspark import SparkContext
#  from operator import add
#  import sys
#  from random import random
#  partitions = 1
#  n = 20000000 * partitions
#  def f(_):
#    x = random() * 2 - 1
#    y = random() * 2 - 1
#    return 1 if x ** 2 + y ** 2 < 1 else 0
#  
#  count = sc.parallelize(range(1, n + 1), partitions).map(f).reduce(add)
#  Pi = (4.0 * count / n)
#  print("Pi is roughly %f" % Pi)'
#  
#  azureSparkCMD(sc, CMD = pythonCmd, sessionID = "0")
#  
#  ## [1] "Pi is roughly 3.140285"

## ---- eval=FALSE---------------------------------------------------------
#  azureSparkCMD(sc, clustername = "smrhdi", CMD = "print Pi", sessionID = "0")
#  
#  #[1] "3.1422"

## ---- eval=FALSE---------------------------------------------------------
#  azureSparkNewSession(sc, clustername = "smrhdi",
#                       hdiAdmin = "hdiadmin",
#                       hdiPassword = "AzureSMR_password123",
#                       kind = "sparkr")
#  azureSparkCMD(sc, clustername = "smrhdi", CMD = "HW<-'hello R'", sessionID = "2")
#  azureSparkCMD(sc, clustername = "smrhdi", CMD = "cat(HW)", sessionID = "2")

## ---- eval=FALSE---------------------------------------------------------
#  asc <- createAzureContext(tenantID = "{TID}", clientID = "{CID}", authKey= "{KEY}")

## ---- eval=FALSE---------------------------------------------------------
#  azureDataLakeMkdirs(asc, azureDataLakeAccount, "tempfolder")

## ---- eval=FALSE---------------------------------------------------------
#  azureDataLakeListStatus(asc, azureDataLakeAccount, "")
#  azureDataLakeListStatus(asc, azureDataLakeAccount, "tempfolder")

## ---- eval=FALSE---------------------------------------------------------
#  azureDataLakeCreate(asc, azureDataLakeAccount, "tempfolder/tempfile00.txt",
#                      "755", FALSE,
#                      4194304L, 3L, 268435456L,
#                      charToRaw("abcd"))

## ---- eval=FALSE---------------------------------------------------------
#  azureDataLakeAppend(asc, azureDataLakeAccount, "tempfolder/tempfile00.txt", 4194304L, charToRaw("stuv"))

## ---- eval=FALSE---------------------------------------------------------
#  azureDataLakeRead(asc, azureDataLakeAccount, "tempfolder/tempfile00.txt",
#                    length = 2L, bufferSize = 4194304L)

## ---- eval=FALSE---------------------------------------------------------
#  azureDataLakeDelete(asc, azureDataLakeAccount, "tempfolder", TRUE)

