package com.kaczmarek.bigdata.serde

import com.kaczmarek.bigdata.model._
import org.apache.kafka.common.serialization.Serde

object CustomSerdes {

    implicit val movieRatingUserAggregateKey: Serde[MovieRatingUserAggregateKey] = serde()
    implicit val movieRatingUserAggregateValue: Serde[MovieRatingUserAggregateValue] = serde()
    implicit val movieRatingAggregateKey: Serde[MovieRatingAggregateKey] = serde()
    implicit val movieRatingAggregateValue: Serde[MovieRatingAggregateValue] = serde()
    implicit val movieRatingResultWithoutTitle: Serde[MovieRatingResultWithoutTitle] = serde()
    implicit val movieRatingResult: Serde[MovieRatingResult] = serde()

    private def serde[T](): Serde[T] = {
        new ObjectSerde[T]
    }
}
