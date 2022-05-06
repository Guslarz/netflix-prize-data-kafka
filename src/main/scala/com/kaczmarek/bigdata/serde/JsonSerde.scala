package com.kaczmarek.bigdata.serde

import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

import scala.reflect.ClassTag

class JsonSerde[T](implicit private val classTag: ClassTag[T]) extends Serde[T] {

    override def serializer(): Serializer[T] = new JsonSerializer[T]

    override def deserializer(): Deserializer[T] = new JsonDeserializer[T]
}
