package com.kaczmarek.bigdata.serde

import com.kaczmarek.bigdata.schema.MessageWithSchema
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

import scala.reflect.ClassTag

class JsonWithSchemaSerde[T](implicit private val classTag: ClassTag[T]) extends Serde[MessageWithSchema[T]] {

    override def serializer(): Serializer[MessageWithSchema[T]] = new JsonSerializer[MessageWithSchema[T]]

    override def deserializer(): Deserializer[MessageWithSchema[T]] = new JsonWithSchemaDeserializer[T]
}
