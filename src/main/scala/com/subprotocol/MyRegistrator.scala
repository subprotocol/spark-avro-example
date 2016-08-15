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

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.subprotocol.MyRegistrator.AvroSerializerWrapper
import com.subprotocol.avro.MyAvroRecord
import org.apache.avro.io.{BinaryDecoder, BinaryEncoder, DecoderFactory, EncoderFactory}
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter, SpecificRecord}
import org.apache.spark.serializer.KryoRegistrator


class MyRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    // register avro specific classes here
    kryo.register(classOf[MyAvroRecord], new AvroSerializerWrapper[MyAvroRecord])
  }
}


object MyRegistrator {

  // this is required for avro to function properly
  class AvroSerializerWrapper[T <: SpecificRecord : Manifest] extends Serializer[T] {

    val reader = new SpecificDatumReader[T](manifest[T].runtimeClass.asInstanceOf[Class[T]])
    val writer = new SpecificDatumWriter[T](manifest[T].runtimeClass.asInstanceOf[Class[T]])
    var encoder = null.asInstanceOf[BinaryEncoder]
    var decoder = null.asInstanceOf[BinaryDecoder]

    setAcceptsNull(false)

    def write(kryo: Kryo, output: Output, record: T) = {
      encoder = EncoderFactory.get().directBinaryEncoder(output, encoder)
      writer.write(record, encoder)
    }

    def read(kryo: Kryo, input: Input, klazz: Class[T]): T = this.synchronized {
      decoder = DecoderFactory.get().directBinaryDecoder(input, decoder)
      reader.read(null.asInstanceOf[T], decoder)
    }
  }
}


