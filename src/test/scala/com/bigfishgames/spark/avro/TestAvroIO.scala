//package com.bigfishgames.spark.avro
//
//import org.apache.avro.generic.GenericRecord
//import org.apache.avro.generic.GenericRecordBuilder
//import org.apache.avro.mapred.AvroKey
//import org.apache.hadoop.io.NullWritable
//import org.apache.spark.rdd.RDD
//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
//import org.scalatest.BeforeAndAfter
//import org.scalatest.FunSuite
//
//import scalax.file.Path
//
//class TestAvroIO extends FunSuite with Serializable with BeforeAndAfter {
//
//  //Create Spark Configuration and Context
//  var sc: SparkContext = _
//
//  before {
//    val path = Path.fromString("src/test/resources/avroIOTest")
//    path.deleteRecursively(continueOnFailure = true)
//
//    val conf = new SparkConf()
//      .setMaster("local[3]")
//      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .set("spark.kryoserializer.buffer", "24")
//      .set("HADOOP_HOME", System.getenv().get("HADOOP_HOME"))
//      .setAppName("test Avro IO")
//
//    sc = new SparkContext(conf)
//  }
//
//  after {
//    val path = Path.fromString("src/test/resources/avroIOTest")
//    path.deleteRecursively(continueOnFailure = true)
//
//    sc.stop
//    System.clearProperty("spark.driver.port")
//  }
//
//  // define the schema used by all tests
//  val avroSchema = """{                              
//              "type": "record",                                            
//              "name": "RPT_GT_EVENT_STREAM",                                         
//              "fields": [      
//              	      {"name": "test1", "type": ["null","string"]},                    
//                      {"name": "test2", "type": ["null","long"]},  
//                      {"name": "test3", "type": ["null","int"]} 
//                      ] }"""
//
//  // instantiate a new AvroIO object
//  //val avroIO = new AvroIO
//  val testPath = "src/test/resources/avroIOTest"
//  
//  // create a new record based on the Avro schema already defined
//  def buildRecord() = {
//    val recordBuilder = new GenericRecordBuilder(AvroIO.parseAvroSchema(avroSchema))
//    recordBuilder.set("test1", "test")
//    recordBuilder.set("test2", 1234566L)
//    recordBuilder.set("test3", 1)
//
//    recordBuilder.build
//  }
//
//  // write the RDD to HDFS
//  def writeRecord(avroRdd: RDD[(AvroKey[GenericRecord], NullWritable)]) = AvroIO.writeAvroFile(avroRdd, avroSchema, testPath, "test-write-avro")
//
//  // read the HDFS file into an RDD
//  def readRecord() = sc.hadoopFile(
//    testPath,
//    classOf[org.apache.avro.mapred.AvroInputFormat[GenericRecord]],
//    classOf[org.apache.avro.mapred.AvroWrapper[GenericRecord]],
//    classOf[org.apache.hadoop.io.NullWritable])
//
//  // test that a single line record can be created, written to HDFS, and read back correctly
//  test("test writing avro file - single line") {
//    
//    val record = buildRecord()
//    writeRecord(sc.parallelize(Seq((new AvroKey[GenericRecord](record), NullWritable.get))))
//    val checkRdd = readRecord()
//
//    checkRdd.collect().foreach(a => (
//      {
//        assert(a._1.datum().get("test1").toString() == "test")
//        assert(a._1.datum().get("test2").toString() == "1234566")
//        assert(a._1.datum().get("test3").toString() == "1")
//      }))
//
//    assert(checkRdd.count() == 1)
//  }
//
//  // test that a two line record can be created, written to HDFS, and read back correctly
//  test("test writing avro file - multiple line") {
//
//    val record = buildRecord()
//    writeRecord(sc.parallelize(Seq((new AvroKey[GenericRecord](record), NullWritable.get), (new AvroKey[GenericRecord](record), NullWritable.get))))
//    val checkRdd = readRecord()
//
//    checkRdd.collect().foreach(a => (
//      {
//        assert(a._1.datum().get("test1").toString() == "test")
//        assert(a._1.datum().get("test2").toString() == "1234566")
//        assert(a._1.datum().get("test3").toString() == "1")
//      }))
//
//    assert(checkRdd.count() == 2)
//  }
//
//  // test that an empty record can be created, written to HDFS, and read back correctly
//  test("test writing avro file - no line") {
//
//    val record = buildRecord()
//    writeRecord(sc.emptyRDD[(AvroKey[GenericRecord], NullWritable)])
//    val checkRdd = readRecord()
//
//    assert(checkRdd.count() == 0)
//  }
//
//}