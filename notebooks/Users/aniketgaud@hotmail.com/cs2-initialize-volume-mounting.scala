// Databricks notebook source
import scala.util.control._

// COMMAND ----------


val configs = Map(
  "fs.azure.account.auth.type" -> "OAuth",
  "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id" -> "883444a2-0338-4816-b4ed-8fd660b955b3",
  "fs.azure.account.oauth2.client.secret" -> dbutils.secrets.get(scope = "iomegaadbsecretescope", key = "appsecret"),
  "fs.azure.account.oauth2.client.endpoint" -> "https://login.microsoftonline.com/f6c3682c-4ee4-40ca-aa83-2155908210c5/oauth2/token")

val mounts = dbutils.fs.mounts()
val mountPath = "/mnt/data"
var isExist: Boolean = false
var outer = new Breaks

for(mount <- mounts) {
  if(mount.mountPoint == mountPath) {
    isExist = true;
    outer.break;
  }
}

if(isExist) {
  println("Volume Mounting for Case Study Data Already Exist!")
}
else {
  dbutils.fs.mount(
    source = "abfss://casestudydatademo@iomegadlsdemo.dfs.core.windows.net/",
    mountPoint = "/mnt/data",
    extraConfigs = configs)
}


// COMMAND ----------

