## ---- eval=FALSE---------------------------------------------------------
#  # Install devtools
#  if(!require("devtools")) install.packages("devtools")
#  devtools::install_github("Microsoft/AzureSMR")
#  library(AzureSMR)

## ---- eval=FALSE---------------------------------------------------------
#  sc <- createAzureContext(tenantID = "{TID}", clientID = "{CID}", authKey= "{KEY}")
#  sc

## ---- eval=FALSE---------------------------------------------------------
#  azureListSubscriptions(sc)
#  

## ---- eval=FALSE---------------------------------------------------------
#  # list resource groups
#  AzureListRG(sc)
#  
#  # list all resources
#  azureListAllResources(sc)
#  
#  azureListAllResources(sc, location = "northeurope")
#  
#  azureListAllResources(sc, type = "Microsoft.Sql/servers", location = "northeurope")
#  
#  azureListAllResources(sc, resourceGroup = "Analytics")
#  
#  azureCreateResourceGroup(sc, resourceGroup = "testme", location = "northeurope")
#  
#  azureDeleteResourceGroup(sc, resourceGroup = "testme")
#  
#  azureListRG(sc)$name
#  

## ---- eval=FALSE---------------------------------------------------------
#  azureListVM(sc, resourceGroup = "AWHDIRG")
#  
#  ##            Name    Location                             Type    OS     State  Admin
#  ## 1         DSVM1 northeurope Microsoft.Compute/virtualMachines Linux Succeeded alanwe
#  
#  azureStartVM(sc, vmName = "DSVM1")
#  azureStopVM(sc, vmName = "DSVM1")

## ---- eval=FALSE---------------------------------------------------------
#  sKey <- AzureSAGetKey(sc, resourceGroup = "Analytics", storageAccount = "analyticsfiles")

## ---- eval=FALSE---------------------------------------------------------
#  azListContainers(sc, storageAccount = "analyticsfiles", containers = "Test")

## ---- eval=FALSE---------------------------------------------------------
#  azureListStorageBlobs(sc, storageAccount = "analyticsfiles", container = "test")

## ---- eval=FALSE---------------------------------------------------------
#  AzurePutBlob(sc, StorageAccount = "analyticsfiles", container = "test",
#               contents = "Hello World",
#               blob = "HELLO")

## ---- eval=FALSE---------------------------------------------------------
#  azureGetBlob(sc, storageAccount = "analyticsfiles", container = "test",
#               blob="HELLO",
#               type="text")

## ---- eval=FALSE---------------------------------------------------------
#  azureListHDI(sc)
#  azureListHDI(sc, resourceGroup ="Analytics")
#  

## ---- eval=FALSE---------------------------------------------------------
#  azureResizeHDI(sc, resourceGroup = "Analytics", clusterName = "{HDIClusterName}",
#                 Role="workernode",Size=2)
#  
#  ## AzureResizeHDI: Request Submitted:  2016-06-23 18:50:57
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
#  ## AzureDeployTemplate: Request Submitted:  2016-06-23 18:50:57
#  ## Resizing(R), Succeeded(S)
#  ## RRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR
#  ## RRRRRRRRRRRRRRRRRRS
#  ## Finished Deployed Sucessfully:  2016-06-23 19:04:43
#  ## Finished:  2016-06-23 19:04:43

## ---- eval=FALSE---------------------------------------------------------
#  azureHiveStatus(sc, clusterName = "{hdicluster}",
#                  hdiAdmin = "admin",
#                  hdiPassword = "********")
#  AzureHiveSQL(sc,
#               CMD = "select * from airports",
#               Path = "wasb://{container}@{hdicluster}.blob.core.windows.net/")
#  
#  stdout <- AzureGetBlob(sc, Container = "test", Blob = "stdout")
#  
#  read.delim(text=stdout,  header=TRUE, fill=TRUE)
#  

## ---- eval=FALSE---------------------------------------------------------
#  azureSparkNewSession(sc, clusterName = "{hdicluster}",
#                       hdiAdmin = "admin",
#                       hdiPassword = "********",
#                       kind = "pyspark")

## ---- eval=FALSE---------------------------------------------------------
#  azureSparkListSessions(sc, clusterName = "{hdicluster}")

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
#  azureSparkCMD(sc, cmd = pythonCmd, sessionID = "5")
#  
#  ## [1] "Pi is roughly 3.140285"

## ---- eval=FALSE---------------------------------------------------------
#  azureSparkCMD(sc, clusterName = "{hdicluster}", cmd = "print Pi", sessionID="5")
#  
#  #[1] "3.1422"

