package com.kaczmarek.bigdata.reducer

import com.kaczmarek.bigdata.model.MovieRatingAggregateValue

class MovieRatingAggregateReducer
    extends ((MovieRatingAggregateValue, MovieRatingAggregateValue) => MovieRatingAggregateValue) {

    override def apply(
        accumulator: MovieRatingAggregateValue,
        value: MovieRatingAggregateValue): MovieRatingAggregateValue =
        MovieRatingAggregateValue(
            voteCount = accumulator.voteCount + value.voteCount,
            ratingSum = accumulator.ratingSum + value.ratingSum,
            uniqueVoterCount = accumulator.uniqueVoterCount + value.uniqueVoterCount
        )
}
