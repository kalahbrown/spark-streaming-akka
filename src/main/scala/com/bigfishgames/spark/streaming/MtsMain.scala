package com.bigfishgames.spark.streaming

import org.apache.spark.SparkConf

import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.flume._

object MtsMain {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[5]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer", "24")
      .set("HADOOP_HOME", System.getenv().get("HADOOP_HOME"))
      .setAppName("test read write avro")
      .set("rootDir", "src/test/resources/actorTest")
      .set("stagePath", "staging")
      .set("appNameDepth", "13")
      .set("installBatchWindowSecs", "30")

    val ssc = new StreamingContext(conf, Seconds(30L))
    val flumeStream = FlumeUtils.createStream(ssc, "localhost", 20000)
    flumeStream.saveAsTextFiles("/user/kalah.brown/mts", "avro")
  }
}