//package com.bigfishgames.spark.avro
//
//import org.apache.avro.generic.GenericRecord
//import org.apache.avro.generic.GenericRecordBuilder
//import org.apache.avro.mapred.AvroKey
//import org.apache.avro.mapred.AvroWrapper
//import org.apache.hadoop.io.NullWritable
//import org.apache.spark.rdd.RDD
//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
//import org.scalatest.BeforeAndAfter
//import org.scalatest.FunSuite
//import org.apache.spark.streaming.{ Seconds, StreamingContext, StreamingContextState }
//import scala.collection.mutable.Queue
//
//import scalax.file.Path
//import java.nio.file.Files
//import java.util.Calendar
//import java.text.SimpleDateFormat
//import org.apache.commons.lang.StringEscapeUtils.escapeJava
//
//class TestAvroIOStreaming extends FunSuite with Serializable with BeforeAndAfter {
//
//  //Create Spark Configuration and Context
//  var sc: SparkContext = _
//  var ssc: StreamingContext = _
//  var rddQueue: Queue[RDD[(AvroWrapper[GenericRecord], NullWritable)]] = _
//
//  //val AvroIO = new AvroIO
//  val testPath = "src/test/resources/AvroIOTest"
//  val path = Path.fromString(testPath)
//  
//      val avroSchema = """{                              
//              "type": "record",                                            
//              "name": "RPT_GT_EVENT_STREAM",                                         
//              "fields": [      
//              	      {"name": "test1", "type": ["null","string"]},                    
//                      {"name": "test2", "type": ["null","long"]},  
//                      {"name": "test3", "type": ["null","int"]} 
//                      ] }"""
//
//
//  def instantiateSourceFile(sc: SparkContext, rCount: Int, avroSchema:  String): RDD[(AvroWrapper[GenericRecord], NullWritable)] = {
//    // create a new record based on the Avro schema already defined
//    def buildRecord() = {
//      val recordBuilder = new GenericRecordBuilder(AvroIO.parseAvroSchema(avroSchema))
//      recordBuilder.set("test1", "test")
//      recordBuilder.set("test2", 1234566L)
//      recordBuilder.set("test3", 1)
//
//      recordBuilder.build
//    }
//
//    def currentTime = {
//      val now = Calendar.getInstance.getTime
//      val dateFormat = new SimpleDateFormat("yyyy-MM-dd-H.m.s.S")
//      dateFormat.format(now)
//    }
//    
//    
//    def makeRdd(): Seq[(AvroKey[GenericRecord], NullWritable)] = {
//      var result = Seq[(AvroKey[GenericRecord], NullWritable)]()
//      
//      for( a <- 1 to rCount) { result :+ Seq((new AvroKey[GenericRecord](buildRecord()), NullWritable.get))}
//      result
//    }
//
//    // write the RDD to HDFS
//    def writeRecord(avroRdd: RDD[(AvroKey[GenericRecord], NullWritable)]) = {
//      val fileName = "test-write-avro" + currentTime
//      AvroIO.writeAvroFile(avroRdd, avroSchema, testPath, fileName)
//    }
//
//    // read the HDFS file into an RDD
//    def readRecord() = sc.hadoopFile(
//      testPath,
//      classOf[org.apache.avro.mapred.AvroInputFormat[GenericRecord]],
//      classOf[org.apache.avro.mapred.AvroWrapper[GenericRecord]],
//      classOf[org.apache.hadoop.io.NullWritable])
//
//    //writeRecord(sc.makeRDD(Seq((new AvroKey[GenericRecord](buildRecord()), NullWritable.get))))
//    writeRecord(sc.makeRDD(makeRdd()))
//    readRecord()
//  }
//
//  private def start(ssc: StreamingContext) = {
//    val state = ssc.getState
//    if (state != StreamingContextState.ACTIVE) {
//      ssc.start
//    }
//  }
//
//  private def stop(ssc: StreamingContext) = {
//    ssc.stop(true, true)
//  }
//
//  before {
//    path.deleteRecursively(continueOnFailure = true)
//    
//    val conf = new SparkConf()
//      .setMaster("local[2]")
//      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .set("spark.kryoserializer.buffer", "24")
//      .set("HADOOP_HOME", System.getenv().get("HADOOP_HOME"))
//      .setAppName("test Avro IO")
// 
//    sc = new SparkContext(conf)
//    ssc = new StreamingContext(sc, Seconds(1))
//
//    rddQueue = new Queue[RDD[(AvroWrapper[GenericRecord], NullWritable)]]()
//  }
//
//  after {
//    stop(ssc)
//    sc.stop
//    
//    //Thread.sleep(5000)
//    
//    System.clearProperty("spark.driver.port")
//
//    //path cleanup must come after context stop; otherwise a warning that the path doesn't exist is generated
//    path.deleteRecursively(continueOnFailure = true)
//  }
//
//  // test that a single line record can be created, written to HDFS, and read back correctly
//  test("test writing avro file - single line") {
//
//    rddQueue += instantiateSourceFile(sc, 1, avroSchema)
//
//    val avroStream = AvroIO.readAvroStream(ssc, testPath, avroSchema, false)
//
//    avroStream.foreachRDD {
//      rdd =>
//        {
//          if (!rdd.isEmpty) {
//            assert(rdd.count() == 1)
//
//            rdd.collect().foreach(a => (
//              {
//                assert(a._1.datum().get("test1").toString() == "test")
//                assert(a._1.datum().get("test2").toString() == "1234566")
//                assert(a._1.datum().get("test3").toString() == "1")
//              }))
//          }
//        }
//    }
//
//    start(ssc)
//    ssc.awaitTerminationOrTimeout(2000)
//    //ssc.awaitTermination
//    //Thread.sleep(5000)
//  }
//  
//  
//  
//  // test that a single line record can be created, written to HDFS, and read back correctly
//  test("test writing avro file - multiple lines") {
//
//    rddQueue += instantiateSourceFile(sc, 3, avroSchema)
//
//    val avroStream = AvroIO.readAvroStream(ssc, testPath, avroSchema, false)
//
//    avroStream.foreachRDD {
//      rdd =>
//        {
//          if (!rdd.isEmpty) {
//            assert(rdd.count() == 3)
//
//            rdd.collect().foreach(a => (
//              {
//                assert(a._1.datum().get("test1").toString() == "test")
//                assert(a._1.datum().get("test2").toString() == "1234566")
//                assert(a._1.datum().get("test3").toString() == "1")
//              }))
//          }
//        }
//    }
//
//    start(ssc)
//    ssc.awaitTerminationOrTimeout(2000)
//    //ssc.awaitTermination
//    //Thread.sleep(5000)
//
//  }
//
//  // test that a two line record can be created, written to HDFS, and read back correctly
//  //  test("test writing avro file - multiple line") {
//  //
//  //    val record = buildRecord()
//  //    writeRecord(sc.parallelize(Seq((new AvroKey[GenericRecord](record), NullWritable.get), (new AvroKey[GenericRecord](record), NullWritable.get))))
//  //    val checkRdd = readRecord()
//  //
//  //    checkRdd.collect().foreach(a => (
//  //      {
//  //        assert(a._1.datum().get("test1").toString() == "test")
//  //        assert(a._1.datum().get("test2").toString() == "1234566")
//  //        assert(a._1.datum().get("test3").toString() == "1")
//  //      }))
//  //
//  //    assert(checkRdd.count() == 2)
//  //  }
//  //
//  //  // test that an empty record can be created, written to HDFS, and read back correctly
//  //  test("test writing avro file - no line") {
//  //
//  //    val record = buildRecord()
//  //    writeRecord(sc.emptyRDD[(AvroKey[GenericRecord], NullWritable)])
//  //    val checkRdd = readRecord()
//  //
//  //    assert(checkRdd.count() == 0)
//  //  }
//
//}