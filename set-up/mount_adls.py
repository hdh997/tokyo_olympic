# Databricks notebook source
#Create app regis
#Get client-id, tenent-id, secret
#Give access control on storage
#add those secrets to azure keyvault
#create secret scope in DataBricks
dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.list(scope="tokyo-olympic-scope")

# COMMAND ----------

storage_account_name = "hdhtokyoolympicdata"
container_name="tokyo-olumpic-data"
client_id = dbutils.secrets.get(scope="tokyo-olympic-scope", key="client-id")
tenant_id = dbutils.secrets.get(scope="tokyo-olympic-scope", key="tenant-id")
client_secret = dbutils.secrets.get(scope="tokyo-olympic-scope", key="secret")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

def mount_adls():
    dbutils.fs.mount(
      source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
      mount_point = f"/mnt/{storage_account_name}/{container_name}",
      extra_configs = configs)

# COMMAND ----------

mount_adls()

# COMMAND ----------

display(dbutils.fs.ls("/mnt/hdhtokyoolympicdata/tokyo-olumpic-data/raw-data"))

# COMMAND ----------

#dbutils.fs.unmount("/mnt/hdhtokyoolympicdata/tokyo-olumpic-data/raw-data")
