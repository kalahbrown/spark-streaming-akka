package com.bigfishgames.spark.streaming

import java.util.concurrent.TimeUnit

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions

import com.bigfishgames.spark.avro.AvroIO
import com.bigfishgames.spark.common.DateTime
import com.bigfishgames.spark.common.HdfsUtil
import com.bigfishgames.spark.transform.Transform

import akka.actor.Actor
import akka.actor.ActorLogging

object SparkStreamingContextActor {
  case object RestartContext
}

class RestartTimeoutException extends Exception {
}

class SparkStreamingContextActor(conf: SparkConf) extends Actor with ActorLogging {

  import SparkStreamingContextActor._

  private var ssc: StreamingContext = _

  override def receive = {
    case RestartContext => throw new RestartTimeoutException
  }

  override def preStart = {
    log.info("SparkStreamingContextActor:preStart")
    initializeContext
    startFileStreams
    startSparkStreaming

    val delay = conf.get("rolldelay").toInt
    val interval = conf.get("rollinterval").toInt 
    context.system.scheduler.schedule(Duration(delay, TimeUnit.SECONDS), Duration(interval, TimeUnit.SECONDS), self, RestartContext)
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

    
    //TODO: remove hardcoded eventName and date for getFileStreamIOPath call
    val hdfsPaths = HdfsUtil.getFileStreamIOPaths(conf.get("rootDir"), conf.get("event"), DateTime.currentDate, conf.get("stagePath"))
    val trans = Transform(conf.get("event"))
    val inputSchemaStr = trans.inputSchemaStr
    val outputSchemaStr = trans.outputSchemaStr
    val fileprefix = conf.get("fileprefix")
    val currentTime = DateTime.currentTime
    
    for ((input, output) <- hdfsPaths) {
      //read in avro format file
      var avroStream = AvroIO.readAvroStream(ssc, input, inputSchemaStr, true)
      //de-dup  
      avroStream.reduceByKey((key, value) => key)
      
      //tranform the event using the transformer fucntion
      log.info("Transforming into Install schema from genernic MTS schema")
      val avroTransStream = avroStream.transform(rdd => trans.transformer(rdd))
      
      //write out new tranformed data in avro format
      avroTransStream.foreachRDD(rdd => {
        if (!rdd.partitions.isEmpty) {
          AvroIO.writeAvroFile(rdd, outputSchemaStr, output, fileprefix + currentTime)
        }
      })
    }
  }
}