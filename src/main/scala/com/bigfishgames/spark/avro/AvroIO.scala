package com.bigfishgames.spark.avro

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.AvroJob
import org.apache.avro.mapreduce.AvroKeyInputFormat
import org.apache.avro.mapreduce.AvroKeyOutputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.streaming.StreamingContext
import org.apache.log4j.Logger

object AvroIO extends Serializable {
  val logger = Logger.getLogger(this.getClass.getName)  

  def readAvroStream(ssc: StreamingContext, inputDirectory: String, inputSchemaStr: String, newFilesOnly: Boolean = true) = {
    logger.debug("reading files from inputDirectory = " + inputDirectory + " for avro file stream")
    ssc.fileStream[AvroKey[GenericRecord], NullWritable, AvroKeyInputFormat[GenericRecord]](
          inputDirectory, (path: Path) => { !(path.getName().startsWith("_") || path.getName().startsWith(".")) }
          ,newFilesOnly, createInputAvroJob(inputSchemaStr).getConfiguration)
  }

  def writeAvroFile(avroRdd: RDD[(AvroKey[GenericRecord], NullWritable)], schemaStr: String, outputDirectory: String, outputFileName: String) = {
    logger.debug("writing avro files to outputDirectory = " + outputDirectory )
    avroRdd.saveAsNewAPIHadoopFile(outputDirectory, classOf[AvroKey[GenericRecord]], classOf[NullWritable], 
          classOf[AvroKeyOutputFormat[GenericRecord]], createOutputAvroJob(schemaStr, outputFileName).getConfiguration)
  }
  
  private def createInputAvroJob(inputSchemaStr: String) = {
    val conf = new Configuration()
    conf.setBoolean("mapreduce.output.fileoutputformat.compress", true)
    conf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec")
    logger.info("""creating Avro Job for reading Avro input  
                      mapreduce.output.fileoutputformat.compress = true, 
                      mapreduce.output.fileoutputformat.compress.codec = org.apache.hadoop.io.compress.SnappyCodec""")
    val job = Job.getInstance(conf)
    val inputSchema = parseAvroSchema(inputSchemaStr)
    AvroJob.setInputKeySchema(job, inputSchema)
    job
  }

  private def createOutputAvroJob(outschemaStr: String, outputFileName: String) = {
    val conf = new Configuration()
    conf.setBoolean("mapreduce.output.fileoutputformat.compress", true)
    conf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec")
    conf.set("avro.mo.config.namedOutput", outputFileName)
    logger.info("""creating Avro Job for writing Avro output  
                      mapreduce.output.fileoutputformat.compress = true, 
                      mapreduce.output.fileoutputformat.compress.codec = org.apache.hadoop.io.compress.SnappyCodec,
                      avro.mo.config.namedOutput = """ + outputFileName)
    val job = Job.getInstance(conf)
    val outputSchema = parseAvroSchema(outschemaStr)
    AvroJob.setOutputKeySchema(job, outputSchema)
    job
  }

  //TODO: read schema from hdfs, see BIE-3880
  def parseAvroSchema(schemaStr: String) = {
    val parse = new Schema.Parser();
    parse.parse(schemaStr)
  }

}