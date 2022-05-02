package com.kaczmarek.bigdata.operator.accumulator

import com.kaczmarek.bigdata.model.{MovieRatingAggregateKey, MovieRatingAggregateValue}
import org.slf4j.LoggerFactory

object MovieRatingAggregator {

    private val logger = LoggerFactory.getLogger(getClass)

    def initializer(): MovieRatingAggregateValue = MovieRatingAggregateValue(
        voteCount = 0,
        ratingSum = 0,
        uniqueVoterCount = 0
    )

    def adder(
        key: MovieRatingAggregateKey,
        value: MovieRatingAggregateValue,
        accumulator: MovieRatingAggregateValue): MovieRatingAggregateValue = {
        logger.info(">> {} {}", accumulator, value: Any)
        val result = MovieRatingAggregateValue(
            voteCount = accumulator.voteCount + value.voteCount,
            ratingSum = accumulator.ratingSum + value.ratingSum,
            uniqueVoterCount = accumulator.uniqueVoterCount + value.uniqueVoterCount
        )
        logger.info("<< {}", result)
        result
    }

    def subtractor(
        key: MovieRatingAggregateKey,
        value: MovieRatingAggregateValue,
        accumulator: MovieRatingAggregateValue): MovieRatingAggregateValue = {
        logger.info(">> {} {}", accumulator, value: Any)
        val result = MovieRatingAggregateValue(
            voteCount = accumulator.voteCount - value.voteCount,
            ratingSum = accumulator.ratingSum - value.ratingSum,
            uniqueVoterCount = accumulator.uniqueVoterCount - value.uniqueVoterCount
        )
        logger.info("<< {}", result)
        result
    }
}
