package com.kaczmarek.bigdata.serde

import org.apache.kafka.common.serialization.Deserializer

import java.io.{ByteArrayInputStream, ObjectInputStream}

class ObjectDeserializer[T] extends Deserializer[T] {

    override def deserialize(s: String, bytes: Array[Byte]): T = {
        val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
        val value = ois.readObject
        ois.close()
        value.asInstanceOf[T]
    }
}
