//package com.bigfishgames.spark.install
//
//import akka.actor.OneForOneStrategy
//import akka.actor.SupervisorStrategy._
//import scala.concurrent.duration._
//import akka.actor.Actor
//import akka.actor.Props
//import akka.actor.ActorSystem
//import akka.actor.ActorRef
//
//import com.typesafe.config.{ Config, ConfigFactory }
//import org.scalatest.{ FlatSpecLike, Matchers, BeforeAndAfterAll }
////import akka.testkit.{ TestActors, TestKit, ImplicitSender, EventFilter }
//import akka.testkit.TestActor
//import akka.testkit.TestKit
//import akka.testkit.ImplicitSender
//import akka.testkit.EventFilter
//import akka.actor.Terminated
//
//class Supervisor extends Actor {
//
//  override val supervisorStrategy =
//    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1 minute) {
//      case _: ArithmeticException      => Resume
//      case _: NullPointerException     => Restart
//      case _: IllegalArgumentException => Stop
//      case _: Exception                => Escalate
//    }
//
//  override def receive = {
//    case p: Props => sender() ! context.actorOf(p)
//  }
//
//}
//
//class Child extends Actor {
//  var state = 0
//  def receive = {
//    case ex: Exception => throw ex
//    case x: Int        => state = x
//    case "get"         => sender() ! state
//    case "alive"       => println("I'm still alive")
//    case "keepon"      => println("keep on keepin on ... Restart the Spark Context")
//  }
//}
//
//class Supervisor2 extends Actor {
//  import akka.actor.OneForOneStrategy
//  import akka.actor.SupervisorStrategy._
//  import scala.concurrent.duration._
//
//  override val supervisorStrategy =
//    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1 minute) {
//      case _: ArithmeticException      => Resume
//      case _: NullPointerException     => Restart
//      case _: IllegalArgumentException => Stop
//      case _: Exception                => Escalate
//    }
//
//  def receive = {
//    case p: Props => sender() ! context.actorOf(p)
//  }
//  // override default to kill all children during restart
//  override def preRestart(cause: Throwable, msg: Option[Any]) { println("PRERESTART: Look at me I'm a preRestartin'")}
//  override def postRestart(reason: Throwable) { println("POSTRESTART: Keep on keepin on in the postRestart") }
//  override def preStart { println("PRESTART: I'm in the preStart baby")
//                      
//                          val child = context.actorOf(Props[Child], "supervisorRestart")
//                          child ! "keepon"
//                          }
//  override def postStop { println("POSTSTOP: Stop Spark Streaming Context!!") }
//}
//
//class FaultHandlingDocSpec(_system: ActorSystem) extends TestKit(_system)
//    with ImplicitSender with FlatSpecLike with Matchers with BeforeAndAfterAll {
//
//  def this() = this(ActorSystem("FaultHandlingDocSpec",
//    ConfigFactory.parseString("""
//      akka {
//        loggers = ["akka.testkit.TestEventListener"]
//        loglevel = "WARNING"
//      }
//      """)))
//
//  override def afterAll {
//    TestKit.shutdownActorSystem(system)
//  }
//
//  "A supervisor" must "apply the chosen strategy for its child" in {
////    val supervisor = system.actorOf(Props[Supervisor], "supervisor")
////    supervisor ! Props[Child]
////    val child = expectMsgType[ActorRef] // retrieve answer from TestKit’s testActor
////
////    child ! 42 // set state to 42
////    child ! "get"
////    expectMsg(42)
////    
////    child ! "alive"
////
////    child ! new ArithmeticException // crash it
////    child ! "get"
////    expectMsg(42)
////    
////    child ! "alive"
////
////    child ! new NullPointerException // crash it harder
////    child ! "get"
////    expectMsg(0)
////    
////    child ! "alive"
////
////    watch(child) // have testActor watch “child”
////    child ! new IllegalArgumentException // break it
////    expectMsgPF() { case Terminated(`child`) => () }
////    
////    child ! "alive"
////
////    supervisor ! Props[Child] // create new child
////    val child2 = expectMsgType[ActorRef]
////    watch(child2)
////    child2 ! "get" // verify it is alive
////    expectMsg(0)
////
////    child2 ! new Exception("CRASH") // escalate failure
////    expectMsgPF() {
////      case t @ Terminated(`child2`) if t.existenceConfirmed => ()
////    }
////    
////    child2 ! "alive"
//
//    val supervisor2 = system.actorOf(Props[Supervisor2], "supervisor2")
//
//    supervisor2 ! Props[Child]
//    val child3 = expectMsgType[ActorRef]
//
//    child3 ! 23
//    child3 ! "get"
//    expectMsg(23)
//
//    child3 ! new NullPointerException
//    child3 ! "get"
//    expectMsg(0)
//    
//    child3 ! "alive"
//    
//    
//
//  }
//
//}
//
