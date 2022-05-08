package com.kaczmarek.bigdata.schema

case class MessageWithSchema[T](
    schema: Schema,
    payload: T
)
