package com.kaczmarek.bigdata.serde

import com.kaczmarek.bigdata.model._
import org.apache.kafka.common.serialization.Serde

object CustomSerdes extends AnyRef {

    implicit val movieRatingUserAggregateKey: Serde[MovieRatingUserAggregateKey] = serde()
    implicit val movieRatingUserAggregateValue: Serde[MovieRatingUserAggregateValue] = serde()
    implicit val movieRatingAggregateKey: Serde[MovieRatingAggregateKey] = serde()
    implicit val movieRatingAggregateValue: Serde[MovieRatingAggregateValue] = serde()
    implicit val movieRatingResultWithoutTitle: Serde[MovieRatingResultWithoutTitle] = serde()

    private def serde[T](): Serde[T] = {
        new ObjectSerde[T]
    }
}
