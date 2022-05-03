package com.kaczmarek.bigdata.operator.mapper

import com.kaczmarek.bigdata.model.{AnomalyJoinedResult, AnomalyResultKey, AnomalyResultValue}

class AnomalyJoinedResultToAnomalyResultMapper extends ((Int, AnomalyJoinedResult) => (AnomalyResultKey, AnomalyResultValue)) {

    override def apply(key: Int, value: AnomalyJoinedResult): (AnomalyResultKey, AnomalyResultValue) = (
        AnomalyResultKey(
            movieId = key,
            windowStart = value.windowStart,
            windowEnd = value.windowEnd
        ),
        AnomalyResultValue(
            title = value.title,
            voteCount = value.voteCount,
            ratingAverage = value.ratingAverage
        )
    )
}
