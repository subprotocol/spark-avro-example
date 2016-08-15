/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.subprotocol

import com.subprotocol.AvroUtil.AvroCompression
import com.subprotocol.avro.MyAvroRecord
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

object SparkAvroExample {
  def main(args: Array[String]): Unit = {

    // setup log4j to suppress some excessive logging
    System.setProperty("log4j.configuration","log4j.configuration")

    // create some arbitrary MyAvroRecord objects from The Matrix
    val initial: Seq[MyAvroRecord] =  Seq(
      MyAvroRecord
        .newBuilder()
        .setFirstName("Agent")
        .setLastName("Smith")
        .setAge(38)
        .setAttributes(Map("suit" -> "true"))
        .build(),
      MyAvroRecord
        .newBuilder()
        .setFirstName("Thomas")
        .setLastName("Anderson")
        .setAge(30)
        .setAttributes(Map(
          "suit" -> "false",
          "alias" -> "Neo")
        )
        .build()
    )

    // create RDD from MyAvroRecord objects
    val dataset: RDD[MyAvroRecord] = sc.parallelize(
      seq = initial,
      numSlices = 1
    )

    // delete datafile if it already exists (so that subsequent runs succeed)
    rmr("/tmp/data.avro")

    // save RDD to /tmp/data.avro
    AvroUtil.write(
      path = "/tmp/data.avro",
      schema = MyAvroRecord.getClassSchema,
      directCommit = true, // set to true for S3
      compress = AvroCompression.AvroDeflate(), // enable deflate compression
      avroRdd = dataset // pass in out dataset
    )

    // read Avro file into RDD
    val rddFromFile: RDD[MyAvroRecord] = AvroUtil.read[MyAvroRecord](
      path = "/tmp/data.avro",
      schema = MyAvroRecord.getClassSchema,
      sc = sc
    ).map(MyAvroRecord.newBuilder(_).build()) // clone after reading from file to prevent buffer reference issues

    // display number of records
    println(s"Number of records in RDD: ${rddFromFile.count()}")

    // take two items from dataset and print out some basic information
    rddFromFile
      .take(2)
      .map(record => s"Name: ${record.getFirstName} ${record.getLastName}  Age: ${record.getAge}")
      .foreach(println)

    // list distinct attributes that were found in the data
    val distinctAttributes: String = rddFromFile
      .flatMap(_.getAttributes.keys)
      .distinct
      .collect
      .mkString(", ")

    println(s"Distinct attributes: $distinctAttributes")
  }

  // create spark context
  val sc: SparkContext = {
    val conf = new SparkConf()
      .setAppName("SparkAvroExample")
      .set("spark.master", "local[1]") // single core
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator","com.subprotocol.MyRegistrator")

    new SparkContext(conf)
  }

  // method that deletes a file if it exists
  def rmr(path: String): Unit = {
    try {
      val fs = FileSystem.newInstance(sc.hadoopConfiguration)
      fs.delete(new Path(path), true)
      fs.close()
    } catch {
      case e: Throwable =>
    }
  }
}
