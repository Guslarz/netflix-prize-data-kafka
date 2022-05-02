package com.kaczmarek.bigdata.serde

import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

class ObjectSerde[T] extends Serde[T] {

    override def serializer(): Serializer[T] = new ObjectSerializer[T]

    override def deserializer(): Deserializer[T] = new ObjectDeserializer[T]
}
