hdi_json <- function(subscriptionID, clustername, location,
                     storageAccount, storageKey,
                     version,
                     componentVersion, kind, vmSize,
                     hivejson,
                     sshUser, sshPassword, adminUser, adminPassword,
                     workers) {
  rserver <- if (kind == "rserver") 
    ',"rserver": {"rstudio": true}\n' else ''
  spark <- if (kind == "spark") 
    paste0('"componentVersion": {"Spark": "', componentVersion, '"},\n') else ""

paste0('
  {
    "id":"/subscriptions/', subscriptionID, 
    '/resourceGroups/Analytics/providers/Microsoft.HDInsight/clusters/', clustername, '",
    "name":"', clustername, '",
    "type":"Microsoft.HDInsight/clusters",
    "location":"', location, '",
    "tags": { "tag1": "value1", "tag2": "value2" },
    "properties": {
    "clusterVersion": "', version, '",
    "osType": "Linux",
    "tier": "standard",
    "clusterDefinition": {',
     spark,
    '"kind": "', kind, '",
    "configurations": {
    "gateway": {
        "restAuthCredential.isEnabled": true,
        "restAuthCredential.username":"', adminUser, '",
        "restAuthCredential.password":"', adminPassword, '"
    },
    "core-site": {
      "fs.defaultFS": "wasb://', clustername, '@', storageAccount, '.blob.core.windows.net",
      "fs.azure.account.key.', storageAccount, '.blob.core.windows.net": "', storageKey, '"
    }',
     rserver, 
     hivejson, 
  '}},
  "computeProfile": {
  "roles": [
  {
    "name": "headnode",
    "targetInstanceCount": 2,
    "hardwareProfile": {
    "vmsize": "', vmSize, '"
  },
  "osProfile": {
    "linuxOperatingSystemProfile": {
    "username": "', sshUser, '",
    "password":"', sshPassword, '"
  }}},
  {
    "name": "edgenode",
    "minInstanceCount": 1,
    "targetInstanceCount": 1,
    "hardwareProfile": {
    "vmSize": "', vmSize, '"
  },
  "osProfile": {
    "linuxOperatingSystemProfile": {
    "username": "', sshUser, '",
    "password": "', sshPassword, '"
  }}},
  {
    "name": "workernode",
    "targetInstanceCount":', workers, ',
    "hardwareProfile": {
    "vmsize": "', vmSize, '"
  },
  "osProfile": {
    "linuxOperatingSystemProfile": {
    "username": "', sshUser, '",
    "password": "', sshPassword, '"
  }}},
  {
    "name": "zookeepernode",
    "targetInstanceCount": 3,
    "hardwareProfile": {
    "vmsize": "Small"
  },
    "osProfile": {
    "linuxOperatingSystemProfile": {
    "username": "', sshUser, '",
    "password": "', sshPassword, '"
  }}}
]
}}}'
)}


hive_json = function(hiveServer, hiveDB, hiveUser, hivePassword) {
  paste0('
  ,"hive-site": {
    "javax.jdo.option.ConnectionDriverName": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "javax.jdo.option.ConnectionURL":"jdbc:sqlserver://', hiveServer, ';database=', hiveDB, ';encrypt=true;trustServerCertificate=true;create=false;loginTimeout=300",
  "javax.jdo.option.ConnectionUserName":"', hiveUser, '",
  "javax.jdo.option.ConnectionPassword":"', hivePassword, '"
  },
  "hive-env": {
    "hive_database": "Existing MSSQL Server database with SQL authentication",
    "hive_database_name": "', hiveDB, '",
    "hive_database_type": "mssql",
    "hive_existing_mssql_server_database": "', hiveDB, '",
    "hive_existing_mssql_server_host":"', hiveServer, '",
    "hive_hostname":"', hiveServer,'"
  }'
  )
}