package com.kaczmarek.bigdata.operator.mapper

import com.kaczmarek.bigdata.model.{AnomalyJoinableResult, AnomalyResultWithoutTitle}
import com.kaczmarek.bigdata.util.DateUtils
import org.apache.kafka.streams.kstream.Windowed

class AnomalyResultWithoutTitleToAnomalyJoinableResultMapper
    extends ((Windowed[Int], AnomalyResultWithoutTitle) => (Int, AnomalyJoinableResult)) {

    override def apply(key: Windowed[Int], value: AnomalyResultWithoutTitle): (Int, AnomalyJoinableResult) = (
        key.key(),
        AnomalyJoinableResult(
            windowStart = DateUtils.formatTimestamp(key.window().start()),
            windowEnd = DateUtils.formatTimestamp(key.window().end()),
            voteCount = value.voteCount,
            ratingAverage = value.ratingAverage
        )
    )
}
