//package com.bigfishgames.spark.install
//
//import akka.actor.OneForOneStrategy
//import akka.actor.SupervisorStrategy._
//import scala.concurrent.duration._
//import akka.actor.Actor
//import akka.actor.Props
//import akka.actor.ActorSystem
//import akka.actor.ActorRef
//import com.typesafe.config.{ Config, ConfigFactory }
//import org.scalatest.{ FlatSpecLike, Matchers, BeforeAndAfterAll }
//import akka.testkit.TestActor
//import akka.testkit.TestKit
//import akka.testkit.ImplicitSender
//import akka.testkit.EventFilter
//import akka.actor.Terminated
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.StreamingContext
//import com.bigfishgames.spark.streaming.MasterActor
//
//class TestAkkaInstall (_system: ActorSystem) extends TestKit(_system)
//    with ImplicitSender with FlatSpecLike with Matchers with BeforeAndAfterAll {
//
//  import MasterActor._
//  
//  def this() = this(ActorSystem("TestAkkaInstall",
//    ConfigFactory.parseString("""
//      akka {
//        loggers = ["akka.testkit.TestEventListener"]
//        loglevel = "DEBUG"
//      }
//      """)))
//  override def afterAll {
//   // TestKit.shutdownActorSystem(system)
//  }
//  
//  "A supervisor" must "apply the chosen strategy for its child" in {
//     val conf = new SparkConf()
//      .setMaster("local[5]")
//      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .set("spark.kryoserializer.buffer", "24")
//      .set("HADOOP_HOME", System.getenv().get("HADOOP_HOME"))
//      .setAppName("test read write avro")
//      .set("rootDir", "src/test/resources/actorTest")
//      .set("stagePath", "staging")
//      .set("appNameDepth", "13")
//      .set("installBatchWindowSecs", "30")
//      .set("event", "install")
//      .set("fileprefix", "mts-install-events")
//      .set("rolldelay", "20")
//      .set("rollinterval", "60")
//      //TODO: This fixes the timezone difference between the paritioning (UTC) and the server file timestamp (PST). see BIE-3764
//      ///     Now we need to think about what happens when the job is restarted. I haven't tested this yet, but it could cause data duplication by reading the same data again.
//      .set("spark.streaming.minRememberDuration", "86400s") 
//    val installRef = system.actorOf(Props(classOf[MasterActor],conf), "InstallActor")
//    println("waiting to intialize the actors")
//    Thread.sleep(60000)
//    println("DONE!!!")
//    
//  }
//
//}