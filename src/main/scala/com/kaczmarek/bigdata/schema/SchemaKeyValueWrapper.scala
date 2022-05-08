package com.kaczmarek.bigdata.schema

class SchemaKeyValueWrapper[K, V](keySchemaFile: String, valueSchemaFile: String)
    extends ((K, V) => (MessageWithSchema[K], MessageWithSchema[V])) {

    private val keyWrapper = new SchemaWrapper[K](keySchemaFile)
    private val valueWrapper = new SchemaWrapper[V](valueSchemaFile)

    override def apply(key: K, value: V): (MessageWithSchema[K], MessageWithSchema[V]) = (
        keyWrapper.apply(key),
        valueWrapper.apply(value)
    )
}
