package com.bigfishgames.spark.install

import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.avro.mapred.AvroKey
import org.apache.hadoop.io.NullWritable
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD

import com.bigfishgames.spark.avro.AvroIO
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object InstallTransform {
   //TODO: add to avroIO class to read from hdfs, see BIE-3880
  val outputSchemaStr = """{                              
              "type": "record",                                            
              "name": "RPT_INSTALL_DETAIL",                                         
              "fields": [      
                      {"name": "install_ts", "type": ["null","long"]},                 
                      {"name": "client_ts", "type": ["null","long"]} ,  
                      {"name": "timezone_offset", "type": ["null","int"]}, 
                      {"name": "bfgudid", "type": ["null","string"]} , 
                      {"name": "bundle_id", "type": ["null","string"]},           
                      {"name": "ip_address", "type": ["null","string"]},  
                      {"name": "bfg_sdk_version", "type": ["null","string"]},   
                      {"name": "device_idiom", "type": ["null","string"]},     
                      {"name": "os_info", "type": ["null","string"]},
                      {"name": "os_type", "type": ["null","string"]},  
                      {"name": "source_type", "type": ["null","string"]},   
                      {"name": "platform", "type": ["null","string"]},     
                      {"name": "processor_type", "type": ["null","string"]},     
                      {"name": "device_model", "type": ["null","string"]},     
                      {"name": "ifa", "type": ["null","string"]},   
                      {"name": "idfv", "type": ["null","string"]},                    
                      {"name": "google_advertising_id", "type": ["null","string"]}, 
                      {"name": "android_id", "type": ["null","string"]} , 
                      {"name": "country_cd", "type": ["null", "string"]},
                      {"name": "language_cd", "type": ["null","string"]},
                      {"name": "rave_id", "type": ["null","string"]},
                      {"name": "app_user_id", "type": ["null","long"]},
                      {"name": "app_store_id", "type": ["null","long"]},
                      {"name": "app_store", "type": ["null","string"]},
                      {"name": "screen_resolution", "type": ["null","string"]},
                      {"name": "device_brand", "type": ["null","string"]},
                      {"name": "os_version", "type": ["null","string"]},
                      {"name": "hmid", "type": ["null","string"]},
                      {"name": "game_center_id", "type": ["null","string"]},
                      {"name": "google_gmail_id", "type": ["null","string"]},
                      {"name": "uid", "type": ["null","long"]},
                      {"name": "game_platform_nm", "type": ["null","string"]},
                      {"name": "operating_system_nm", "type": ["null","string"]},
                      {"name": "hardware_manufacturer", "type": ["null","string"]},
                      {"name": "hardware_model_nm", "type": ["null","string"]},
                      {"name": "hardware_model_nbr", "type": ["null","string"]}
                      ] }"""
  val inputSchemaStr = """{
                  "type": "record",
                  "name": "RPT_MTS_EVENT",
                  "fields": [
                          {"name": "eventType", "type": ["null","string"]},
                          {"name": "appName", "type": ["null","string"]},
                          {"name": "timestampServer", "type": ["null","long"]},
                          {"name": "timestampClient", "type": ["null","long"]},
                          {"name": "clientTimezoneOffset", "type": ["null","int"]},
                          {"name": "bfgudid", "type": ["null","string"]},
                          {"name": "raveId", "type": ["null","string"]},
                          {"name": "appUserId", "type": ["null","long"]},
                          {"name": "platform", "type": ["null","string"]},
                          {"name": "appVersion", "type": ["null","string"]},
                          {"name": "appBuildVersion", "type": ["null","string"]},
                          {"name": "sessionId", "type": ["null","string"]},
                          {"name": "playSessionId", "type": ["null","string"]},
                          {"name": "sessionNumber", "type": ["null","int"]},
                          {"name": "ip", "type": ["null","string"]},
                          {"name": "countryCode", "type": ["null","string"]},
                          {"name": "languageCode", "type": ["null","string"]},
                          {"name": "bfgSdkVersion", "type": ["null","string"]},
                          {"name": "msgPayloadVersion", "type": ["null","string"]},
                          {"name": "eventId", "type": ["null","string"]},
                          {"name": "data", "type": ["null","string"]},
                          {"name": "headers", "type": ["null","string"], "default": null},
                          {"name": "timestampLoad", "type": ["null","long"], "default": null}
                      ]
              }"""
  
}

class InstallTransform extends Serializable {

  import InstallTransform._

  def parseInstallInput(avroRdd: RDD[(AvroKey[GenericRecord], NullWritable)]): RDD[(AvroKey[GenericRecord], NullWritable)] = {
    avroRdd.map(row => parse(row))
  }

  private def parse(row: (AvroKey[GenericRecord], NullWritable)) = {
    val key = row._1
        val installTs = Option(key.datum().get("timestampServer")).getOrElse(0L)
        val clientTs = Option(key.datum().get("timestampClient")).getOrElse(0L)
        val timezoneOffset = Option(key.datum().get("clientTimezoneOffset")).getOrElse(0)
        val bfgudid = Option(key.datum().get("bfgudid")).getOrElse(null)
        val ipAddress = Option(key.datum().get("ip")).getOrElse(null)
        val platform = Option(key.datum().get("platform")).getOrElse(null)
        val countryCd = Option(key.datum().get("countryCode")).getOrElse(null)
        val languageCd = Option(key.datum().get("languageCode")).getOrElse(null)
        val raveId = Option(key.datum().get("raveId")).getOrElse(null)
        val appUserId  = Option(key.datum().get("appUserId")).getOrElse(0L)
        val bfgSdkVersion = Option(key.datum().get("bfgSdkVersion")).getOrElse(null)

    println("appUserId = " + appUserId)

    val mapper = new ObjectMapper()
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper.registerModule(DefaultScalaModule)

    val jsonMap = mapper.readValue(key.datum().get("data").toString(), classOf[Map[String, String]])

    val deviceIdiom = jsonMap.getOrElse("deviceIdiom", null)
    val osInfo = jsonMap.getOrElse("osInfo", null)
    val processorType = jsonMap.getOrElse("processorType", null)
    //TODO: fix inline warnings, BIE-3882
    val deviceModel = jsonMap.getOrElse("deviceModel", null)
    val bundleId = jsonMap.getOrElse("bundleId", null)
    val appStore = jsonMap.getOrElse("appStore", null)
    val appStoreId = jsonMap.getOrElse("appStoreId", 0L)
    val screenResolution = jsonMap.getOrElse("screenResolution", null)
    val deviceBrand = jsonMap.getOrElse("deviceBrand", null)
    val osVersion = jsonMap.getOrElse("osVersion", null)

    println("appStoreId" + appStoreId)

    val osName = osNameTransform(osInfo)

    val deviceInfo = jsonMap.get("deviceInfo").orNull.asInstanceOf[Map[String, String]]
    val ifa = deviceInfo.getOrElse("ifa", null)
    val idfv = deviceInfo.getOrElse("idfv", null)
    val googleAdvertisingId = deviceInfo.getOrElse("googleAdvertisingId", null)
    val androidId = deviceInfo.getOrElse("androidId", null)
    val hmid = deviceInfo.getOrElse("hmid", null)
    val gameCenterId = deviceInfo.getOrElse("gameCenterId", null)
    val googleGmailId = deviceInfo.getOrElse("googleGmailId", null)
    val uid = deviceInfo.getOrElse("uid", 0L)

    val hardwareManufacturer = hardwareManufacturerTransform(platform)

    //val avroIO = new AvroIO

    val record = new GenericRecordBuilder(AvroIO.parseAvroSchema(outputSchemaStr))
      .set("install_ts", castToLong(installTs))
      .set("client_ts", castToLong(clientTs))
      .set("timezone_offset", timezoneOffset)
      .set("bfgudid", bfgudid)
      .set("bundle_id", bundleId)
      .set("ip_address", ipAddress)
      .set("bfg_sdk_version", bfgSdkVersion)
      .set("device_idiom", deviceIdiom)
      .set("os_info", osInfo)
      .set("os_type", osName)
      .set("source_type", "Primary")
      .set("platform", platform)
      .set("processor_type", processorType)
      .set("device_model", deviceModel)
      .set("ifa", ifa)
      .set("idfv", idfv)
      .set("google_advertising_id", googleAdvertisingId)
      .set("android_id", androidId)
      .set("country_cd", countryCd)
      .set("language_cd", languageCd)
      .set("rave_id", raveId)
      .set("app_user_id", castToLong(appUserId))
      .set("app_store", appStore)
      .set("app_store_id", castToLong(appStoreId))
      .set("screen_resolution", screenResolution)
      .set("device_brand", deviceBrand)
      .set("os_version", osVersion)
      .set("hmid", hmid)
      .set("game_center_id", gameCenterId)
      .set("google_gmail_id", googleGmailId)
      .set("uid", castToLong(uid))
      .set("game_platform_nm", platform)
      .set("operating_system_nm", osName)
      .set("hardware_manufacturer", hardwareManufacturer)
      .set("hardware_model_nm", processorType)
      .set("hardware_model_nbr", processorType)
      .build()

    (new AvroKey[GenericRecord](record), NullWritable.get)

  }

  def osNameTransform(osInfo: String) = {
    if (osInfo == null) null
    else if (osInfo.toUpperCase().contains("IPHONE") || osInfo.toUpperCase().contains("IPAD") || osInfo.toUpperCase().contains("IPOD")) "IOS"
    else if (osInfo.toUpperCase().contains("ANDROID")) "ANDROID"
    else if (osInfo.toUpperCase().contains("WINDOWS")) "WINDOWS"
    else "UNKNOWN"
  }

  def hardwareManufacturerTransform(platform: Any) = {
    if (platform == null) null
    else if (platform.toString().toUpperCase() == "IOS") "APPLE"
    else "UNKNOWN"
  }

  //TODO: look for a short way or scala-like for this cast, see BIE-3883
  def castToLong(value: Any) = {
    print(value)
    var st: java.lang.Long = 0L

    if (value.isInstanceOf[Long]) {
      println(" Long")
      st = new java.lang.Long(value.asInstanceOf[Long])
    } else if (value.isInstanceOf[java.math.BigInteger]) {
      println(" BigInteger")
      //If we want a Big Integer, change the avro schema to use java.math.BigInteger and cast to that instead of Long
      throw new Exception("BigInteger is not supported. Long is supported")
    } else if (value.isInstanceOf[String]) {
      println(" String")
      st = new java.lang.Long(value.toString)
    } else if (value.isInstanceOf[Int]) {
      println(" Int")
      st = new java.lang.Long(value.asInstanceOf[Int].intValue)
    }
    st
  }
}


