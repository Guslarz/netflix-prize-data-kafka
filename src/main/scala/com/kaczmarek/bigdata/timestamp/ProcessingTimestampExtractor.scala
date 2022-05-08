package com.kaczmarek.bigdata.timestamp

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.streams.processor.TimestampExtractor

class ProcessingTimestampExtractor extends TimestampExtractor {

    override def extract(consumerRecord: ConsumerRecord[AnyRef, AnyRef], l: Long): Long =
        System.currentTimeMillis()
}
