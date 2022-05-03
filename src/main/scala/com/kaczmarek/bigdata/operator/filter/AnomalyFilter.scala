package com.kaczmarek.bigdata.operator.filter

import com.kaczmarek.bigdata.model.{AnomalyResultWithoutTitle, Params}
import org.apache.kafka.streams.kstream.Windowed

class AnomalyFilter(private val params: Params) extends ((Windowed[Int], AnomalyResultWithoutTitle) => Boolean) {

    override def apply(key: Windowed[Int], result: AnomalyResultWithoutTitle): Boolean =
        result.voteCount >= params.anomalyMinimumVoteCount &&
            result.ratingAverage >= params.anomalyMinimumRatingAverage
}
