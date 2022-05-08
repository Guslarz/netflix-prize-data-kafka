package com.kaczmarek.bigdata.util

import com.kaczmarek.bigdata.model.Params
import com.kaczmarek.bigdata.timestamp.ProcessingTimestampExtractor
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
        config.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
            classOf[ProcessingTimestampExtractor])
        config
    }
}
