package com.kaczmarek.bigdata.util

import org.apache.kafka.streams.{KafkaStreams, Topology}

import java.util.Properties
import java.util.concurrent.CountDownLatch

object KafkaRunner {

    def run(topology: Topology, config: Properties): Unit = {
        val streams = new KafkaStreams(topology, config)
        val latch = new CountDownLatch(1)

        Runtime.getRuntime.addShutdownHook(new Thread("streams-shutdown-hook") {
            override def run(): Unit = {
                streams.close()
                latch.countDown()
            }
        })

        try {
            streams.start()
            latch.await()
        } catch {
            case _: Throwable =>
                System.exit(1)
        }
        System.exit(0)
    }
}
