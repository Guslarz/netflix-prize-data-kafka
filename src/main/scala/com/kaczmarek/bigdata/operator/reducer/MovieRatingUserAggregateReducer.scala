package com.kaczmarek.bigdata.operator.reducer

import com.kaczmarek.bigdata.model.MovieRatingUserAggregateValue

class MovieRatingUserAggregateReducer
    extends ((MovieRatingUserAggregateValue, MovieRatingUserAggregateValue) => MovieRatingUserAggregateValue) {

    override def apply(
        accumulator: MovieRatingUserAggregateValue,
        value: MovieRatingUserAggregateValue): MovieRatingUserAggregateValue =
        MovieRatingUserAggregateValue(
            voteCount = accumulator.voteCount + value.voteCount,
            ratingSum = accumulator.ratingSum + value.ratingSum
        )
}
