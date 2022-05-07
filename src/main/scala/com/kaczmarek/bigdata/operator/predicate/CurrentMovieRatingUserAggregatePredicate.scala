package com.kaczmarek.bigdata.operator.predicate

import com.kaczmarek.bigdata.model.{MovieRatingUserAggregateKey, MovieRatingUserAggregateValue}

import java.time.LocalDate

class CurrentMovieRatingUserAggregatePredicate
    extends ((MovieRatingUserAggregateKey, MovieRatingUserAggregateValue) => Boolean) {

    override def apply(key: MovieRatingUserAggregateKey, value: MovieRatingUserAggregateValue): Boolean = {
        val now = LocalDate.now
        val year = now.getYear
        val month = now.getMonthValue
        key.year > year || (key.year == year && key.month >= month)
    }
}
