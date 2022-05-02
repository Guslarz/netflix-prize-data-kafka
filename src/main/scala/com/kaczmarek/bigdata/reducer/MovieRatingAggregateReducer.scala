package com.kaczmarek.bigdata.reducer

import com.kaczmarek.bigdata.model.MovieRatingAggregateValue
import org.slf4j.LoggerFactory

class MovieRatingAggregateReducer
    extends ((MovieRatingAggregateValue, MovieRatingAggregateValue) => MovieRatingAggregateValue) {

    private val logger = LoggerFactory.getLogger(getClass)

    override def apply(
        accumulator: MovieRatingAggregateValue,
        value: MovieRatingAggregateValue): MovieRatingAggregateValue = {
        logger.info(">> {} {}", accumulator, value: Any)
        val result = MovieRatingAggregateValue(
            voteCount = accumulator.voteCount + value.voteCount,
            ratingSum = accumulator.ratingSum + value.ratingSum,
            uniqueVoterCount = accumulator.uniqueVoterCount + value.uniqueVoterCount
        )
        logger.info("<< {}", result)
        result
    }
}
