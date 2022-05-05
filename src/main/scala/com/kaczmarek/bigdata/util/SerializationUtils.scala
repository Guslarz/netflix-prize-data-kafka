package com.kaczmarek.bigdata.util

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object SerializationUtils {

    val objectMapper: ObjectMapper = createObjectMapper()

    private def createObjectMapper(): ObjectMapper = {
        val mapper = new ObjectMapper
        mapper.registerModule(DefaultScalaModule)
        mapper
    }
}
