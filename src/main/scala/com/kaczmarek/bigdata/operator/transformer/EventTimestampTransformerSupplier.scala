package com.kaczmarek.bigdata.operator.transformer

import com.kaczmarek.bigdata.model.MovieRatingVote
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.{Transformer, TransformerSupplier}
import org.apache.kafka.streams.processor.{ProcessorContext, To}

class EventTimestampTransformerSupplier
    extends TransformerSupplier[String, MovieRatingVote, KeyValue[String, MovieRatingVote]] {

    override def get(): Transformer[String, MovieRatingVote, KeyValue[String, MovieRatingVote]] =
        new Transformer[String, MovieRatingVote, KeyValue[String, MovieRatingVote]] {
            var context: ProcessorContext = _

            override def init(context: ProcessorContext): Unit = {
                this.context = context
            }

            override def transform(key: String, value: MovieRatingVote): KeyValue[String, MovieRatingVote] = {
                context.forward(key, value, To.all().withTimestamp(value.date.getTime))
                null
            }

            override def close(): Unit = {}
        }
}
