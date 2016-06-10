package com.bigfishgames.spark.transform

import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.AvroKey
import org.apache.hadoop.io.NullWritable
import org.apache.spark.rdd.RDD

import javax.naming.ConfigurationException

trait Transform {
  def transformer(avroRdd: RDD[(AvroKey[GenericRecord], NullWritable)]): RDD[(AvroKey[GenericRecord], NullWritable)]
  def inputSchemaStr:String
  def outputSchemaStr:String
}

object Transform {
  def apply(t: String): Transform = {

    if (t == "install")
      return  new InstallTransform
    else
      throw new ConfigurationException(t + " is not a supported event")
  }

}