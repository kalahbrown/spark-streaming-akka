//package com.bigfishgames.spark.install
//
//import org.scalatest.BeforeAndAfter
//import org.scalatest.FunSuite
//import com.bigfishgames.spark.install
//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
//import org.apache.avro.generic.GenericRecord
//import org.apache.avro.generic.GenericRecordBuilder
//import org.apache.avro.mapred.AvroKey
//import org.apache.hadoop.io.NullWritable
//import com.bigfishgames.spark.avro.AvroIO
//
//class TestInstallTransform extends FunSuite with Serializable with BeforeAndAfter {
//
//  //Create Spark Configuration and Context
//  var sc: SparkContext = _
//  
//  val trans = new InstallTransform
//  val inputstream = trans.inputSchemaStr
//  
//  before {
//    val conf = new SparkConf()
//      .setMaster("local[5]")
//      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .set("spark.kryoserializer.buffer", "24")
//      .set("HADOOP_HOME", System.getenv().get("HADOOP_HOME"))
//      .setAppName("test Avro IO")
//
//    sc = new SparkContext(conf)
//  }
//
//  after {
//    sc.stop
//    System.clearProperty("spark.driver.port")
//  }
//
//  test("test install parsing, valid data") {
//
//    //val AvroIO = new AvroIO
//
//    val recordBuilder = new GenericRecordBuilder(AvroIO.parseAvroSchema(inputstream))
//    recordBuilder.set("eventType", "install")
//    recordBuilder.set("appName", "gummydrop")
//    recordBuilder.set("timestampServer", 1437955200L)
//    recordBuilder.set("timestampClient", 1437955201L)
//    recordBuilder.set("clientTimezoneOffset", 7)
//    recordBuilder.set("bfgudid", "d839ac578a674d04b9ae989ba9467f29")
//    recordBuilder.set("raveId", "d839ac578a674d04b9ae989ba9467f27")
//    recordBuilder.set("appUserId", 12321321542L)
//    recordBuilder.set("platform", "ios")
//    recordBuilder.set("appVersion", "1.1.0")
//    recordBuilder.set("appBuildVersion", "1.2.0")
//    recordBuilder.set("sessionId", "d839ac578a674d04b9ae989ba9467f21")
//    recordBuilder.set("playSessionId", "d839ac578a674d04b9ae989ba9467f25")
//    recordBuilder.set("sessionNumber", 2)
//    recordBuilder.set("ip", "123.456.789.1")
//    recordBuilder.set("countryCode", "US")
//    recordBuilder.set("languageCode", "en-US")
//    recordBuilder.set("bfgSdkVersion", "06000000")
//    recordBuilder.set("msgPayloadVersion", "1.1")
//    recordBuilder.set("eventId", "54947df8-0e9e-4471-a2f9-9af509fb5889")
//    recordBuilder.set("data", """{
//                        "deviceInfo":{
//                          "gameCenterId":"e000e8b1494fc74dcd6135323fcc191a99233394",
//                          "ifa":"ABBBB3A0-13F8-4B68-AC0E-3B657CF7317E",
//                          "idfv":"CCCCC3A0-13F8-4B68-AC0E-3B657CF7317E"
//                        },
//                        "appStore": "itunes",
//                        "appStoreId": "123456789",
//                        "bundleId": "com.bigfishgames.bfgsdkios",
//                        "processorType": "mobile",
//                        "screenResolution": "768x1024",
//                        "osInfo": "iPhone OS",
//                        "osVersion": "7.1.1",
//                        "deviceCarrier": "Verizon",
//                        "deviceModel": "iPhone6,1",
//                        "deviceBrand": "iPhone",
//                        "deviceIdiom": "phone"
//                      }""")
//    recordBuilder.set("headers", "foo")
//    recordBuilder.set("timestampLoad", 1437955202L)
//
//    val record = recordBuilder.build
//
//    val avroRdd = sc.parallelize(Seq((new AvroKey[GenericRecord](record), NullWritable.get)))
//
//    
//    val finalRdd = trans.parseInstallInput(avroRdd)
//
//    val aRecord = finalRdd.first
//    val key = aRecord._1
//
//    assert(key.datum().get("install_ts") == 1437955200L)
//    assert(key.datum().get("client_ts") == 1437955201L)
//    assert(key.datum().get("timezone_offset") == 7)
//    assert(key.datum().get("bfgudid") == "d839ac578a674d04b9ae989ba9467f29")
//    assert(key.datum().get("bundle_id") == "com.bigfishgames.bfgsdkios")
//    assert(key.datum().get("ip_address") == "123.456.789.1")
//    assert(key.datum().get("bfg_sdk_version") == "06000000")
//    assert(key.datum().get("device_idiom") == "phone")
//    assert(key.datum().get("os_info") == "iPhone OS")
//    assert(key.datum().get("os_type") == "IOS")
//    assert(key.datum().get("source_type") == "Primary")
//    assert(key.datum().get("platform") == "ios")
//    assert(key.datum().get("processor_type") == "mobile")
//    assert(key.datum().get("device_model") == "iPhone6,1")
//    assert(key.datum().get("ifa") == "ABBBB3A0-13F8-4B68-AC0E-3B657CF7317E")
//    assert(key.datum().get("idfv") == "CCCCC3A0-13F8-4B68-AC0E-3B657CF7317E")
//    assert(key.datum().get("google_advertising_id") == null)
//    assert(key.datum().get("android_id") == null)
//    assert(key.datum().get("country_cd") == "US")
//    assert(key.datum().get("language_cd") == "en-US")
//    assert(key.datum().get("rave_id") == "d839ac578a674d04b9ae989ba9467f27")
//    assert(key.datum().get("app_user_id") == 12321321542L)
//    assert(key.datum().get("app_store") == "itunes")
//    assert(key.datum().get("app_store_id") == 123456789L)
//    assert(key.datum().get("screen_resolution") == "768x1024")
//    assert(key.datum().get("device_brand") == "iPhone")
//    assert(key.datum().get("os_version") == "7.1.1")
//    assert(key.datum().get("hmid") == null)
//    assert(key.datum().get("game_center_id") == "e000e8b1494fc74dcd6135323fcc191a99233394")
//    assert(key.datum().get("google_gmail_id") == null)
//    assert(key.datum().get("uid") == 0L)
//    assert(key.datum().get("game_platform_nm") == "ios")
//    assert(key.datum().get("operating_system_nm") == "IOS")
//    assert(key.datum().get("hardware_manufacturer") == "APPLE")
//    assert(key.datum().get("hardware_model_nm") == "mobile")
//    assert(key.datum().get("hardware_model_nbr") == "mobile")
//
//    assert(finalRdd.count == 1)
//
//  }
//  
//   test("test install parsing, empty data") {
//
//    //val AvroIO = new AvroIO
//
//    val recordBuilder = new GenericRecordBuilder(AvroIO.parseAvroSchema(inputstream))
//    recordBuilder.set("eventType", null)
//    recordBuilder.set("appName", null)
//    recordBuilder.set("timestampServer", null)
//    recordBuilder.set("timestampClient", null)
//    recordBuilder.set("clientTimezoneOffset", null)
//    recordBuilder.set("bfgudid", null)
//    recordBuilder.set("raveId", null)
//    recordBuilder.set("appUserId", null)
//    recordBuilder.set("platform", null)
//    recordBuilder.set("appVersion", null)
//    recordBuilder.set("appBuildVersion", null)
//    recordBuilder.set("sessionId", null)
//    recordBuilder.set("playSessionId", null)
//    recordBuilder.set("sessionNumber", null)
//    recordBuilder.set("ip", null)
//    recordBuilder.set("countryCode", null)
//    recordBuilder.set("languageCode", null)
//    recordBuilder.set("bfgSdkVersion", null)
//    recordBuilder.set("msgPayloadVersion", null)
//    recordBuilder.set("eventId", null)
//    recordBuilder.set("data", """{
//                        "deviceInfo":{
//                          "gameCenterId":null,
//                          "ifa":null,
//                          "idfv":null
//                        },
//                        "appStore": null,
//                        "appStoreId": null,
//                        "bundleId": null,
//                        "processorType": null,
//                        "screenResolution": null,
//                        "osInfo": null,
//                        "osVersion": null,
//                        "deviceCarrier": null,
//                        "deviceModel": null,
//                        "deviceBrand": null,
//                        "deviceIdiom": null
//                      }""")
//    recordBuilder.set("headers", null)
//    recordBuilder.set("timestampLoad", null)
//
//    val record = recordBuilder.build
//
//    val avroRdd = sc.parallelize(Seq((new AvroKey[GenericRecord](record), NullWritable.get)))
//
//    
//    val finalRdd = trans.parseInstallInput(avroRdd)
//
//    val aRecord = finalRdd.first
//    val key = aRecord._1
//
//    assert(key.datum().get("install_ts") == 0L)
//    assert(key.datum().get("client_ts") == 0L)
//    assert(key.datum().get("timezone_offset") == 0)
//    assert(key.datum().get("bfgudid") == null)
//    assert(key.datum().get("bundle_id") == null)
//    assert(key.datum().get("ip_address") == null)
//    assert(key.datum().get("bfg_sdk_version") == null)
//    assert(key.datum().get("device_idiom") == null)
//    assert(key.datum().get("os_info") == null)
//    assert(key.datum().get("os_type") == null)
//    assert(key.datum().get("source_type") == "Primary")
//    assert(key.datum().get("platform") == null)
//    assert(key.datum().get("processor_type") == null)
//    assert(key.datum().get("device_model") == null)
//    assert(key.datum().get("ifa") == null)
//    assert(key.datum().get("idfv") == null)
//    assert(key.datum().get("google_advertising_id") == null)
//    assert(key.datum().get("android_id") == null)
//    assert(key.datum().get("country_cd") == null)
//    assert(key.datum().get("language_cd") == null)
//    assert(key.datum().get("rave_id") == null)
//    assert(key.datum().get("app_user_id") == 0L)
//    assert(key.datum().get("app_store") == null)
//    assert(key.datum().get("app_store_id") == 0L)
//    assert(key.datum().get("screen_resolution") == null)
//    assert(key.datum().get("device_brand") == null)
//    assert(key.datum().get("os_version") == null)
//    assert(key.datum().get("hmid") == null)
//    assert(key.datum().get("game_center_id") == null)
//    assert(key.datum().get("google_gmail_id") == null)
//    assert(key.datum().get("uid") == 0L)
//    assert(key.datum().get("game_platform_nm") == null)
//    assert(key.datum().get("operating_system_nm") == null)
//    assert(key.datum().get("hardware_manufacturer") == null)
//    assert(key.datum().get("hardware_model_nm") == null)
//    assert(key.datum().get("hardware_model_nbr") == null)
//
//    assert(finalRdd.count == 1)
//
//  }
//   
//
//  test("test os_type transform android") {
//    //val AvroIO = new AvroIO
//
//    val recordBuilder = new GenericRecordBuilder(AvroIO.parseAvroSchema(inputstream))
//    recordBuilder.set("eventType", "install")
//    recordBuilder.set("appName", "gummydrop")
//    recordBuilder.set("timestampServer", 1437955200L)
//    recordBuilder.set("timestampClient", 1437955201L)
//    recordBuilder.set("clientTimezoneOffset", 7)
//    recordBuilder.set("bfgudid", "d839ac578a674d04b9ae989ba9467f29")
//    recordBuilder.set("raveId", "d839ac578a674d04b9ae989ba9467f27")
//    recordBuilder.set("appUserId", 12321321542L)
//    recordBuilder.set("platform", "android")
//    recordBuilder.set("appVersion", "1.1.0")
//    recordBuilder.set("appBuildVersion", "1.2.0")
//    recordBuilder.set("sessionId", "d839ac578a674d04b9ae989ba9467f21")
//    recordBuilder.set("playSessionId", "d839ac578a674d04b9ae989ba9467f25")
//    recordBuilder.set("sessionNumber", 2)
//    recordBuilder.set("ip", "123.456.789.1")
//    recordBuilder.set("countryCode", "US")
//    recordBuilder.set("languageCode", "en-US")
//    recordBuilder.set("bfgSdkVersion", "06000000")
//    recordBuilder.set("msgPayloadVersion", "1.1")
//    recordBuilder.set("eventId", "54947df8-0e9e-4471-a2f9-9af509fb5889")
//    recordBuilder.set("data", """{
//                        "deviceInfo":{
//                          "gameCenterId":"e000e8b1494fc74dcd6135323fcc191a99233394",
//                          "ifa":"ABBBB3A0-13F8-4B68-AC0E-3B657CF7317E",
//                          "idfv":"CCCCC3A0-13F8-4B68-AC0E-3B657CF7317E"
//                        },
//                        "appStore": "google",
//                        "appStoreId": "123456789",
//                        "bundleId": "com.bigfishgames.bfgsdkios",
//                        "processorType": "mobile",
//                        "screenResolution": "768x1024",
//                        "osInfo": "Android OS",
//                        "osVersion": "7.1.1",
//                        "deviceCarrier": "Verizon",
//                        "deviceModel": "galaxy",
//                        "deviceBrand": "galaxy",
//                        "deviceIdiom": "phone"
//                      }""")
//    recordBuilder.set("headers", "foo")
//    recordBuilder.set("timestampLoad", 1437955202L)
//
//    val record = recordBuilder.build
//
//    val avroRdd = sc.parallelize(Seq((new AvroKey[GenericRecord](record), NullWritable.get)))
//
//    val finalRdd = trans.parseInstallInput(avroRdd)
//
//    val aRecord = finalRdd.first
//    val key = aRecord._1
//
//    assert(key.datum().get("os_type") == "ANDROID")
//    assert(key.datum().get("operating_system_nm") == "ANDROID")
//    assert(key.datum().get("hardware_manufacturer") == "UNKNOWN")
//
//    assert(finalRdd.count == 1)
//  }
//
//  test("test os_type transform ipad") {
//    //val installTransform = new InstallTransform
//    //val AvroIO = new AvroIO
//
//    val recordBuilder = new GenericRecordBuilder(AvroIO.parseAvroSchema(inputstream))
//    recordBuilder.set("eventType", "install")
//    recordBuilder.set("appName", "gummydrop")
//    recordBuilder.set("timestampServer", 1437955200L)
//    recordBuilder.set("timestampClient", 1437955201L)
//    recordBuilder.set("clientTimezoneOffset", 7)
//    recordBuilder.set("bfgudid", "d839ac578a674d04b9ae989ba9467f29")
//    recordBuilder.set("raveId", "d839ac578a674d04b9ae989ba9467f27")
//    recordBuilder.set("appUserId", 12321321542L)
//    recordBuilder.set("platform", "ios")
//    recordBuilder.set("appVersion", "1.1.0")
//    recordBuilder.set("appBuildVersion", "1.2.0")
//    recordBuilder.set("sessionId", "d839ac578a674d04b9ae989ba9467f21")
//    recordBuilder.set("playSessionId", "d839ac578a674d04b9ae989ba9467f25")
//    recordBuilder.set("sessionNumber", 2)
//    recordBuilder.set("ip", "123.456.789.1")
//    recordBuilder.set("countryCode", "US")
//    recordBuilder.set("languageCode", "en-US")
//    recordBuilder.set("bfgSdkVersion", "06000000")
//    recordBuilder.set("msgPayloadVersion", "1.1")
//    recordBuilder.set("eventId", "54947df8-0e9e-4471-a2f9-9af509fb5889")
//    recordBuilder.set("data", """{
//                        "deviceInfo":{
//                          "gameCenterId":"e000e8b1494fc74dcd6135323fcc191a99233394",
//                          "ifa":"ABBBB3A0-13F8-4B68-AC0E-3B657CF7317E",
//                          "idfv":"CCCCC3A0-13F8-4B68-AC0E-3B657CF7317E"
//                        },
//                        "appStore": "itunes",
//                        "appStoreId": "123456789",
//                        "bundleId": "com.bigfishgames.bfgsdkios",
//                        "processorType": "mobile",
//                        "screenResolution": "768x1024",
//                        "osInfo": "iPad OS",
//                        "osVersion": "7.1.1",
//                        "deviceCarrier": "Verizon",
//                        "deviceModel": "iPhone6,1",
//                        "deviceBrand": "iPhone",
//                        "deviceIdiom": "phone"
//                      }""")
//    recordBuilder.set("headers", "foo")
//    recordBuilder.set("timestampLoad", 1437955202L)
//
//    val record = recordBuilder.build
//
//    val avroRdd = sc.parallelize(Seq((new AvroKey[GenericRecord](record), NullWritable.get)))
//
//    val finalRdd = trans.parseInstallInput(avroRdd)
//    
//
//    val aRecord = finalRdd.first
//    val key = aRecord._1
//
//    assert(key.datum().get("os_type") == "IOS")
//    assert(key.datum().get("operating_system_nm") == "IOS")
//
//    assert(finalRdd.count == 1)
//  }
//
//  test("test os_type transform ipod") {
//    //val AvroIO = new AvroIO
//
//    val recordBuilder = new GenericRecordBuilder(AvroIO.parseAvroSchema(inputstream))
//    recordBuilder.set("eventType", "install")
//    recordBuilder.set("appName", "gummydrop")
//    recordBuilder.set("timestampServer", 1437955200L)
//    recordBuilder.set("timestampClient", 1437955201L)
//    recordBuilder.set("clientTimezoneOffset", 7)
//    recordBuilder.set("bfgudid", "d839ac578a674d04b9ae989ba9467f29")
//    recordBuilder.set("raveId", "d839ac578a674d04b9ae989ba9467f27")
//    recordBuilder.set("appUserId", 12321321542L)
//    recordBuilder.set("platform", "ios")
//    recordBuilder.set("appVersion", "1.1.0")
//    recordBuilder.set("appBuildVersion", "1.2.0")
//    recordBuilder.set("sessionId", "d839ac578a674d04b9ae989ba9467f21")
//    recordBuilder.set("playSessionId", "d839ac578a674d04b9ae989ba9467f25")
//    recordBuilder.set("sessionNumber", 2)
//    recordBuilder.set("ip", "123.456.789.1")
//    recordBuilder.set("countryCode", "US")
//    recordBuilder.set("languageCode", "en-US")
//    recordBuilder.set("bfgSdkVersion", "06000000")
//    recordBuilder.set("msgPayloadVersion", "1.1")
//    recordBuilder.set("eventId", "54947df8-0e9e-4471-a2f9-9af509fb5889")
//    recordBuilder.set("data", """{
//                        "deviceInfo":{
//                          "gameCenterId":"e000e8b1494fc74dcd6135323fcc191a99233394",
//                          "ifa":"ABBBB3A0-13F8-4B68-AC0E-3B657CF7317E",
//                          "idfv":"CCCCC3A0-13F8-4B68-AC0E-3B657CF7317E"
//                        },
//                        "appStore": "itunes",
//                        "appStoreId": "123456789",
//                        "bundleId": "com.bigfishgames.bfgsdkios",
//                        "processorType": "mobile",
//                        "screenResolution": "768x1024",
//                        "osInfo": "iPod OS",
//                        "osVersion": "7.1.1",
//                        "deviceCarrier": "Verizon",
//                        "deviceModel": "iPhone6,1",
//                        "deviceBrand": "iPhone",
//                        "deviceIdiom": "phone"
//                      }""")
//    recordBuilder.set("headers", "foo")
//    recordBuilder.set("timestampLoad", 1437955202L)
//
//    val record = recordBuilder.build
//
//    val avroRdd = sc.parallelize(Seq((new AvroKey[GenericRecord](record), NullWritable.get)))
//
//    val finalRdd = trans.parseInstallInput(avroRdd)
//
//    val aRecord = finalRdd.first
//    val key = aRecord._1
//
//    assert(key.datum().get("os_type") == "IOS")
//    assert(key.datum().get("operating_system_nm") == "IOS")
//
//    assert(finalRdd.count == 1)
//  }
//}