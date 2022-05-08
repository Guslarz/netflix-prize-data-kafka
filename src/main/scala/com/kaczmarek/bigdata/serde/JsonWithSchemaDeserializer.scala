package com.kaczmarek.bigdata.serde

import com.fasterxml.jackson.core.`type`.TypeReference
import com.kaczmarek.bigdata.schema.MessageWithSchema
import com.kaczmarek.bigdata.util.SerializationUtils
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Deserializer

import scala.reflect.ClassTag

class JsonWithSchemaDeserializer[T](implicit classTag: ClassTag[T]) extends Deserializer[MessageWithSchema[T]] {

    override def deserialize(topic: String, bytes: Array[Byte]): MessageWithSchema[T] = {
        if (bytes == null) {
            return null.asInstanceOf[MessageWithSchema[T]]
        }

        try {
            // jackson error - deserializes payload as map
            val message = SerializationUtils.objectMapper.readValue(bytes,
                new TypeReference[MessageWithSchema[Map[String, String]]] {})
                .asInstanceOf[MessageWithSchema[T]]
            val payloadBytes = SerializationUtils.objectMapper.writeValueAsBytes(message.payload)
            MessageWithSchema[T](
                message.schema,
                SerializationUtils.objectMapper.readValue(payloadBytes, classTag.runtimeClass).asInstanceOf[T]
            )
        } catch {
            case e: Exception =>
                throw new SerializationException("Error deserializing JSON message", e)
        }
    }
}
