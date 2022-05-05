package com.kaczmarek.bigdata.serde

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.kaczmarek.bigdata.util.SerializationUtils
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Deserializer

import scala.reflect.ClassTag

class JsonDeserializer[T](implicit classTag: ClassTag[T]) extends Deserializer[T] {

    override def deserialize(topic: String, bytes: Array[Byte]): T = {
        if (bytes == null) {
            return null.asInstanceOf[T]
        }

        try {
            SerializationUtils.objectMapper.readValue(bytes, classTag.runtimeClass).asInstanceOf[T]
        } catch {
            case e: Exception =>
                throw new SerializationException("Error deserializing JSON message", e)
        }
    }
}
