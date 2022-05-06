package com.kaczmarek.bigdata.timestamp

import com.kaczmarek.bigdata.model.MovieRatingVote
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.streams.processor.TimestampExtractor

class MovieRatingVoteTimestampExtractor extends TimestampExtractor {

    override def extract(record: ConsumerRecord[AnyRef, AnyRef], previousTimestamp: Long): Long = Some(record)
        .filter(_.value().isInstanceOf[MovieRatingVote])
        .map(_.value().asInstanceOf[MovieRatingVote])
        .map(_.date.getTime)
        .getOrElse(-1)
}
