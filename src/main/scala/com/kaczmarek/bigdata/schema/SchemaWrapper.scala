package com.kaczmarek.bigdata.schema

import com.kaczmarek.bigdata.util.SerializationUtils

import scala.io.Source

class SchemaWrapper[T](schemaFile: String) extends (T => MessageWithSchema[T]) {

    private val schema: Schema = readSchemaFromResources(schemaFile)

    override def apply(value: T): MessageWithSchema[T] = MessageWithSchema(
        schema = schema,
        payload = value
    )

    private def readSchemaFromResources(filename: String): Schema = {
        SerializationUtils.objectMapper.readValue(
            Source.fromInputStream(getClass.getResourceAsStream(filename))
                .getLines()
                .mkString,
            classOf[Schema])
    }
}
