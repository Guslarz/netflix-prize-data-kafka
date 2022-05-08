package com.kaczmarek.bigdata.schema

case class Schema(
    name: String,
    `type`: String,
    properties: List[SchemaProperty]
)
