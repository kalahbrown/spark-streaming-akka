package com.bigfishgames.spark.install

import java.io.IOException
import java.util.concurrent.TimeUnit

import scala.concurrent.duration.Duration

import org.apache.spark.SparkConf

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.AllForOneStrategy
import akka.actor.Props
import akka.actor.SupervisorStrategy.Restart

object InstallActor {
  case object StartInstall
}

class InstallActor(conf: SparkConf) extends Actor with ActorLogging {

  import InstallActor._
  import SparkStreamingContextActor._

  //TODO: look at the testing akka framework, maybe need to put a prop actor here
  override def receive = {
    case StartInstall => log.info("starting install")
  }

  override def preStart = {
    log.info("preStart InstallActor")
    val streamingContextActor = context.actorOf(Props(classOf[SparkStreamingContextActor], conf), name = "SparkStreamingContext")
  }

  override def postStop = {
    log.info("postStop InstallActor")
    context.system.shutdown
  }

  //TODO: Add more Exceptions and appropriate actions to take
  override val supervisorStrategy =
    AllForOneStrategy(maxNrOfRetries = 1, withinTimeRange = Duration(30, TimeUnit.SECONDS)) {
      case _: RestartTimeoutException => Restart
      case _: IOException             => Restart
    }
}