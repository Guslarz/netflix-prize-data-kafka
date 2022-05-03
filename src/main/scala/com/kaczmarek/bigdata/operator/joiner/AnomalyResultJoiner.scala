package com.kaczmarek.bigdata.operator.joiner

import com.kaczmarek.bigdata.model.{AnomalyJoinableResult, AnomalyJoinedResult}

class AnomalyResultJoiner extends ((AnomalyJoinableResult, String) => AnomalyJoinedResult) {
    override def apply(result: AnomalyJoinableResult, title: String): AnomalyJoinedResult =
        AnomalyJoinedResult(
            windowStart = result.windowStart,
            windowEnd = result.windowEnd,
            title = title,
            voteCount = result.voteCount,
            ratingAverage = result.ratingAverage
        )
}
