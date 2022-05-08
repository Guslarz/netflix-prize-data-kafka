package com.kaczmarek.bigdata.serde

import com.kaczmarek.bigdata.model._
import com.kaczmarek.bigdata.schema.MessageWithSchema
import org.apache.kafka.common.serialization.Serde

object CustomSerdes {

    val movieRatingVoteInput: Serde[MovieRatingVote] = new CsvSerde
    val movieInput: Serde[Movie] = new CsvSerde
    implicit val movieRatingVote: Serde[MovieRatingVote] = new ObjectSerde
    implicit val movieRatingUserAggregateKey: Serde[MovieRatingUserAggregateKey] = new ObjectSerde
    implicit val movieRatingUserAggregateValue: Serde[MovieRatingUserAggregateValue] = new ObjectSerde
    implicit val movieRatingAggregateKey: Serde[MovieRatingAggregateKey] = new ObjectSerde
    implicit val movieRatingAggregateValue: Serde[MovieRatingAggregateValue] = new ObjectSerde
    implicit val movieRatingResultWithoutTitle: Serde[MovieRatingResultWithoutTitle] = new ObjectSerde
    implicit val movieRatingResultKey: Serde[MovieRatingResultKey] = new ObjectSerde
    implicit val movieRatingResultValue: Serde[MovieRatingResultValue] = new ObjectSerde
    implicit val anomalyAggregate: Serde[AnomalyAggregate] = new ObjectSerde
    implicit val anomalyResultWithoutTitle: Serde[AnomalyResultWithoutTitle] = new ObjectSerde
    implicit val anomalyJoinableResult: Serde[AnomalyJoinableResult] = new ObjectSerde
    implicit val anomalyJoinedResult: Serde[AnomalyJoinedResult] = new ObjectSerde
    implicit val anomalyResultKey: Serde[AnomalyResultKey] = new ObjectSerde
    implicit val anomalyResultValue: Serde[AnomalyResultValue] = new ObjectSerde
    val movieRatingResultKeyJson: Serde[MovieRatingResultKey] = new JsonSerde
    val movieRatingResultValueJson: Serde[MovieRatingResultValue] = new JsonSerde
    val anomalyResultKeyJson: Serde[AnomalyResultKey] = new JsonSerde
    val anomalyResultValueJson: Serde[AnomalyResultValue] = new JsonSerde
    val movieRatingResultKeySchemaJson: Serde[MessageWithSchema[MovieRatingResultKey]] = new JsonWithSchemaSerde
    val movieRatingResultValueSchemaJson: Serde[MessageWithSchema[MovieRatingResultValue]] = new JsonWithSchemaSerde
    val anomalyResultKeySchemaJson: Serde[MessageWithSchema[AnomalyResultKey]] = new JsonWithSchemaSerde
    val anomalyResultValueSchemaJson: Serde[MessageWithSchema[AnomalyResultValue]] = new JsonWithSchemaSerde
}
