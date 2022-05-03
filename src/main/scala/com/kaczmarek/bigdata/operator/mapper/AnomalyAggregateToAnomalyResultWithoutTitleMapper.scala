package com.kaczmarek.bigdata.operator.mapper

import com.kaczmarek.bigdata.model.{AnomalyAggregate, AnomalyResultWithoutTitle}

class AnomalyAggregateToAnomalyResultWithoutTitleMapper extends (AnomalyAggregate => AnomalyResultWithoutTitle) {

    override def apply(value: AnomalyAggregate): AnomalyResultWithoutTitle =
        AnomalyResultWithoutTitle(
            voteCount = value.voteCount,
            ratingAverage = value.ratingSum.toDouble / value.voteCount
        )
}
