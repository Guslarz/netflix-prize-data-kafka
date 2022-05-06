package com.kaczmarek.bigdata.util

import com.kaczmarek.bigdata.model.Params
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler

import java.util.Properties

object KafkaConfigCreator {

    def createConfig(params: Params): Properties = {
        val config = new Properties()
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, params.server)
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "netflix-prize-data")
        config.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
            classOf[LogAndContinueExceptionHandler])
        config
    }
}
