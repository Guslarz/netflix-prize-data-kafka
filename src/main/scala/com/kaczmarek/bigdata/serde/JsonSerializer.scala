package com.kaczmarek.bigdata.serde

import com.fasterxml.jackson.databind.ObjectMapper
import com.kaczmarek.bigdata.util.SerializationUtils
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Serializer


class JsonSerializer[T] extends Serializer[T] {

    private val objectMapper = new ObjectMapper

    override def serialize(topic: String, data: T): Array[Byte] = {
        if (data == null) {
            return null
        }

        try {
            SerializationUtils.objectMapper.writeValueAsBytes(data)
        } catch {
            case e: Exception =>
                throw new SerializationException("Error serializing JSON message", e)
        }
    }
}
