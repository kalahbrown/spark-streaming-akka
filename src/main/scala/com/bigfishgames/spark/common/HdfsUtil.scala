package com.bigfishgames.spark.common

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration

import scala.collection.mutable.Map

object HdfsUtil {
  //TODO: add log4j logging
  //TODO: could we use URI instead of building the path manually

  /**
   * The input data is written to hdfs by flume into a staging directory
   * root   = the root dir where flume writes the data to hdfs
   * date   = the day partition in hdfs
   * staged = the directory where input files are staged by flume
   * @returns a Map (InputDir -> OutputDir)
   */
  def getFileStreamIOPaths(root: String, eventType: String, date: String, staged: String) = {
    val fs = FileSystem.get(new Configuration)
    val status = fs.listStatus(new Path(root))
    val fileStreamPathMap: Map[String, String] = Map()

    status.foreach{x=>println(x.getPath)}
    status.foreach {x=>fileStreamPathMap += ((x.getPath.toString() + "/" + eventType +  "/" + date + "/" + staged) -> (x.getPath.toString() +  "/" + eventType +  "/" + date)) }
    fileStreamPathMap
  }

}