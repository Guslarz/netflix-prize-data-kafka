package com.kaczmarek.bigdata.util

import com.kaczmarek.bigdata.model.Params
import org.apache.kafka.streams.StreamsConfig

import java.util.Properties

object KafkaConfigCreator {

    def createConfig(params: Params): Properties = {
        val config = new Properties()
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, params.server)
        config
    }
}
