package com.kaczmarek.bigdata

import org.apache.kafka.streams.Topology
import org.slf4j.{Logger, LoggerFactory}

import java.util.Properties
import com.kaczmarek.bigdata.model.Params
import com.kaczmarek.bigdata.parser.ParamsParser
import com.kaczmarek.bigdata.util.{KafkaConfigCreator, KafkaTopologyCreator}

object NetflixPrizeDataKafka {

    private val logger: Logger = LoggerFactory.getLogger(NetflixPrizeDataKafka.getClass)
    private val paramsParser = new ParamsParser

    def main(args: Array[String]): Unit = {
        val params: Params = paramsParser.parse(args)
        val topology: Topology = KafkaTopologyCreator.createTopology(params)
        val config: Properties = KafkaConfigCreator.createConfig(params)

        logger.info("{}", params)
        logger.info("{}", topology.describe())

        //        KafkaRunner.run(topology, config)
    }
}
