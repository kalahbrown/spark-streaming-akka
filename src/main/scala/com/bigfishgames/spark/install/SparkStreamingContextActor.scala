package com.bigfishgames.spark.install

import java.util.concurrent.TimeUnit

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions

import com.bigfishgames.spark.avro.AvroIO
import com.bigfishgames.spark.common.DateTime
import com.bigfishgames.spark.common.HdfsUtil

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.actorRef2Scala

object SparkStreamingContextActor {
  case object RestartContext
}

class RestartTimeoutException extends Exception {

}

class SparkStreamingContextActor(conf: SparkConf) extends Actor with ActorLogging {

  import SparkStreamingContextActor._

  private var ssc: StreamingContext = _
  private var fileStreamCount: Int = _
  private var maxFileStreamCount: Int = _

  override def receive = {
    case RestartContext => throw new RestartTimeoutException
  }

  override def preStart = {
    log.info("SparkStreamingContextActor:preStart")
    initializeContext
    startFileStreams
    startSparkStreaming

    context.system.scheduler.schedule(Duration(20, TimeUnit.SECONDS), Duration(86400, TimeUnit.SECONDS), self, RestartContext)
  }

  override def postStop = {
    log.info("SparkStreamingContextActor:postStop")
    stopSparkStreaming
    //TODO: after stoping how do we insure files won't be processed again on the restart?, the set("spark.streaming.minRememberDuration", "86400s") 
    //      this work is captured in BIE-3764
  }

  private def initializeContext = {
    log.info("SparkStreamingContextActor:initializeContext, Creating a New StreamingContext")
    ssc = new StreamingContext(conf, Seconds(conf.get("installBatchWindowSecs").toLong))
    log.info("SparkStreamingContextActor:initializeContext, StreamingContext State is " + ssc.getState)

  }

  private def stopSparkStreaming = {
    log.info("SparkStreamingContextActor:stopSparkStreaming, stopping Spark Streaming context")
    log.info("SparkStreamingContextActor:stopSparkStreaming, streaming context state before stopping is " + ssc.getState())

    val state = ssc.getState

    //TODO: add IllegalState exception
    if (state != "STOPPED" && state != "INITIALIZED") {
      ssc.stop(true)
      System.clearProperty("spark.driver.port")
      log.info("SparkStreamingContextActor:stopSparkStreaming, streaming context state  after stopping is " + ssc.getState())
    }
  }

  private def startSparkStreaming = {
    log.info("SparkStreamingContextActor:startSparkStreaming context = " + ssc.getState)
    log.info("SparkStreamingContextActor:startSparkStreaming, starting Spark Streaming context")
    //TODO: add IllegalState exception
    ssc.start
  }

  private def startFileStreams = {
    log.info("FileStreamActor:startFileStream, Spark Context state is " + ssc.getState)

    log.info("startFileStreams, Creating a FileStream foreach app")
    log.info("SparkStreamingContextActor: startFileStream, Context state is " + ssc.getState)
    //TODO: remove hardcoded eventName and date for getFileStreamIOPath call
    val hdfsPaths = HdfsUtil.getFileStreamIOPaths(conf.get("rootDir"), "install", "2016-01-01", conf.get("stagePath"))
    for ((input, output) <- hdfsPaths) {

      var avroStream = AvroIO.readAvroStream(ssc, input, InstallTransform.inputSchemaStr, true)
      val trans = new InstallTransform

      avroStream.reduceByKey((key, value) => key)
      log.info("Transforming into Install schema from genernic MTS schema")
      val avroTransStream = avroStream.transform(rdd => trans.parseInstallInput(rdd))
      avroTransStream.foreachRDD(rdd => {
        if (!rdd.partitions.isEmpty) {
          AvroIO.writeAvroFile(rdd, InstallTransform.outputSchemaStr, output, "mts-install-events-" + DateTime.currentTime)
        }
      })
    }
  }
}