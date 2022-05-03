package com.kaczmarek.bigdata.operator.reducer

import com.kaczmarek.bigdata.model.AnomalyAggregate

class AnomalyAggregateReducer extends ((AnomalyAggregate, AnomalyAggregate) => AnomalyAggregate) {

    override def apply(accumulator: AnomalyAggregate, value: AnomalyAggregate): AnomalyAggregate =
        AnomalyAggregate(
            voteCount = accumulator.voteCount + value.voteCount,
            ratingSum = accumulator.ratingSum + value.ratingSum
        )
}
