package com.kaczmarek.bigdata.serde

import org.apache.kafka.common.serialization.Serializer

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

class ObjectSerializer[T] extends Serializer[T] {

    override def serialize(topic: String, value: T): Array[Byte] = {
        val stream: ByteArrayOutputStream = new ByteArrayOutputStream
        val oos = new ObjectOutputStream(stream)
        oos.writeObject(value)
        oos.close()
        stream.toByteArray
    }
}
