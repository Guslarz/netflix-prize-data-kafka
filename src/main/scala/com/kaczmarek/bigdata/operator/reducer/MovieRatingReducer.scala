package com.kaczmarek.bigdata.operator.reducer

import com.kaczmarek.bigdata.model.MovieRatingAggregateValue

object MovieRatingReducer {

    def adder(
        accumulator: MovieRatingAggregateValue,
        value: MovieRatingAggregateValue): MovieRatingAggregateValue =
        MovieRatingAggregateValue(
            voteCount = accumulator.voteCount + value.voteCount,
            ratingSum = accumulator.ratingSum + value.ratingSum,
            uniqueVoterCount = accumulator.uniqueVoterCount + value.uniqueVoterCount
        )

    def subtractor(
        accumulator: MovieRatingAggregateValue,
        value: MovieRatingAggregateValue): MovieRatingAggregateValue =
        MovieRatingAggregateValue(
            voteCount = accumulator.voteCount - value.voteCount,
            ratingSum = accumulator.ratingSum - value.ratingSum,
            uniqueVoterCount = accumulator.uniqueVoterCount - value.uniqueVoterCount
        )
}
