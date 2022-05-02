package com.kaczmarek.bigdata.operator.accumulator

import com.kaczmarek.bigdata.model.{MovieRatingAggregateKey, MovieRatingAggregateValue}

object MovieRatingAggregator {

    def initializer(): MovieRatingAggregateValue = MovieRatingAggregateValue(
        voteCount = 0,
        ratingSum = 0,
        uniqueVoterCount = 0
    )

    def adder(
        key: MovieRatingAggregateKey,
        value: MovieRatingAggregateValue,
        accumulator: MovieRatingAggregateValue): MovieRatingAggregateValue =
        MovieRatingAggregateValue(
            voteCount = accumulator.voteCount + value.voteCount,
            ratingSum = accumulator.ratingSum + value.ratingSum,
            uniqueVoterCount = accumulator.uniqueVoterCount + value.uniqueVoterCount
        )

    def subtractor(
        key: MovieRatingAggregateKey,
        value: MovieRatingAggregateValue,
        accumulator: MovieRatingAggregateValue): MovieRatingAggregateValue =
        MovieRatingAggregateValue(
            voteCount = accumulator.voteCount - value.voteCount,
            ratingSum = accumulator.ratingSum - value.ratingSum,
            uniqueVoterCount = accumulator.uniqueVoterCount - value.uniqueVoterCount
        )
}
