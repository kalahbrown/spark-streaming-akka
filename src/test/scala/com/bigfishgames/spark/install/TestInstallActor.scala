//package com.bigfishgames.spark.install
//
//import java.io.File
//import java.io.Serializable
//import java.nio.file.FileSystems
//import java.nio.file.Files
//import java.nio.file.StandardCopyOption
//import java.text.SimpleDateFormat
//import java.util.Calendar
//
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.StreamingContext
//import org.scalatest.BeforeAndAfter
//import org.scalatest.FunSuite
//
//import akka.actor.Actor
//import akka.actor.ActorLogging
//import akka.actor.ActorRef
//import akka.actor.ActorSystem
//import akka.actor.Props
//import akka.actor.Terminated
//import akka.actor.actorRef2Scala
//import scalax.file.Path
//
//import com.bigfishgames.spark.streaming.MasterActor
//import com.bigfishgames.spark.transform.InstallTransform;
//
//class Terminator(ref: ActorRef) extends Actor with ActorLogging {
//  context watch ref
//    def receive = {
//      case Terminated(_) =>
//        log.info("{} has terminated, shutting down system", ref.path)
//        context.system.shutdown
//    }
//  
//}
//
//class TestMasterActor extends FunSuite with Serializable with BeforeAndAfter {
//  var ssc: StreamingContext = _
//  var conf: SparkConf = _
//  val trans = new InstallTransform
//  val todaysDate = currentDate
//  val testPath = "src/test/resources/test1/install/" + todaysDate
//  val path = Path.fromString(testPath)
// // val avroIO = new AvroIO
//  //val outputAvroSchema = trans.outputSchemaStr
//  val system = ActorSystem("InstallEtlSystem")
//  
//
//  before {
////    conf = new SparkConf()
////      .setMaster("local[5]")
////      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
////      .set("spark.kryoserializer.buffer", "24")
////      .set("HADOOP_HOME", System.getenv().get("HADOOP_HOME"))
////      .setAppName("test read write avro")
////
////    ssc = new StreamingContext(conf, Seconds(30))
////    path.deleteRecursively(continueOnFailure = true)
//  }
//
//  after {
//    System.clearProperty("spark.driver.port")
//    path.deleteRecursively(continueOnFailure = true)
//  }
//  
//  test("Install Actor Test") {
//    //ssc, "src/test/resources/test1", "src/test/resources"
//  
//    
//    println("Starting the Install Actor Test")
//    
//    val conf = new SparkConf()
//      .setMaster("local[5]")
//      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .set("spark.kryoserializer.buffer", "24")
//      .set("HADOOP_HOME", System.getenv().get("HADOOP_HOME"))
//      .setAppName("test read write avro")
//      .set("installInputDir", "src/test/resources/test1")
//      .set("installOutputDir", "src/test/resources/output")
//      .set("installBatchWindowSecs", "30")
//      
//    val MasterActor = system.actorOf(Props(classOf[MasterActor], conf), "Install")
//    Thread.sleep(20000)
//   // MasterActor ! TryThisRestartShit
//    Thread.sleep(20000)
//
////    println("Instantiated the Install Actor")
////    println("Starting the Actor System")
//      
////    println("Let's wait a bit more ...")
////    Thread.sleep(120000)
//    
//  }
//  
//
////  test("read write avro") {
////    val inputPath = testPath + "/staging/"
////    val outputPath = testPath + "/"
////    val fileToProcess = new File(testPath + "/staging/mtsEvent.1461665296716.avro")
////    val system = ActorSystem("InstallEtlSystem")
////    val actor = system.actorOf(Props(classOf[MasterActor], ssc, inputPath, outputPath, true))
////    val fileTime = FileTime.fromMillis((System.currentTimeMillis() + 60000))
////
////    createTestDir
////    copyFile
////
////    actor ! "run"
////    Thread.sleep(10000)
////    Files.setLastModifiedTime(fileToProcess.toPath, fileTime)
////
////    ssc.start
////    ssc.awaitTerminationOrTimeout(120000)
////    ssc.stop(true, true)
////
////    assert(outputFileCount(outputPath) > 0)
////  }
//
//  private def currentDate = {
//    val today = Calendar.getInstance.getTime
//    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
//    dateFormat.format(today)
//  }
//
//  private def createTestDir() {
//    (new File(testPath + "/staging/")).mkdirs()
//  }
//
//  private def copyFile() {
//    val sys = FileSystems.getDefault()
//    val original = sys.getPath("src/test/rawdata/mtsEvent.1461665296716.avro")
//    val input = sys.getPath(testPath + "/staging/mtsEvent.1461665296716.avro")
//    Files.copy(original, input, StandardCopyOption.REPLACE_EXISTING)
//    println("copied file from resources to input")
//  }
//
//  private def outputFileCount(outputDirString: String) = {
//    new java.io.File(outputDirString).listFiles.filter(_.getName.startsWith("mts-install-event")).filter(_.getName.endsWith(".avro")).length
//  }
//}