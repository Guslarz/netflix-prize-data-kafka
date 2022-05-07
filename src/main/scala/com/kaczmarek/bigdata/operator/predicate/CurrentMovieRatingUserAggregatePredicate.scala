package com.kaczmarek.bigdata.operator.predicate

import com.kaczmarek.bigdata.model.{MovieRatingUserAggregateKey, MovieRatingUserAggregateValue}
import com.kaczmarek.bigdata.util.DateUtils

class CurrentMovieRatingUserAggregatePredicate
    extends ((MovieRatingUserAggregateKey, MovieRatingUserAggregateValue) => Boolean) {

    override def apply(key: MovieRatingUserAggregateKey, value: MovieRatingUserAggregateValue): Boolean =
        DateUtils.yearMonthToTimestamp(key.year, key.month) >= System.currentTimeMillis()
}
