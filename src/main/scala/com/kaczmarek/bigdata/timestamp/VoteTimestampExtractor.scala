package com.kaczmarek.bigdata.timestamp

import com.kaczmarek.bigdata.parser.MovieRatingVoteParser
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.streams.processor.TimestampExtractor

class VoteTimestampExtractor extends TimestampExtractor {

    private val movieRatingVoteParser = new MovieRatingVoteParser

    override def extract(record: ConsumerRecord[AnyRef, AnyRef], previousTimestamp: Long): Long = Some(record)
        .filter(_.value().isInstanceOf[String])
        .map(_.value().asInstanceOf[String])
        .flatMap(movieRatingVoteParser.tryParse)
        .map(_.date.getTime)
        .getOrElse(-1)
}
