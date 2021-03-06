package com.kaczmarek.bigdata.schema

case class Schema(
    name: String,
    `type`: String,
    fields: List[SchemaField],
    optional: Boolean
)
