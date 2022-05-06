package com.kaczmarek.bigdata.serde

import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Serializer

import java.lang.reflect.Field

class CsvSerializer[T] extends Serializer[T] {

    override def serialize(topic: String, data: T): Array[Byte] = {
        try {
            data
                .getClass
                .getDeclaredFields
                .map(getValue(data, _))
                .mkString(",")
                .getBytes
        } catch {
            case e: Exception => throw new SerializationException("Failed to serialize", e)
        }
    }

    private def getValue(data: T, field: Field): String = {
        field.setAccessible(true)
        val value = field.get(data)
        field.setAccessible(false)
        value.toString
    }
}
