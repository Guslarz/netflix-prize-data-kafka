package com.kaczmarek.bigdata.operator.reducer

import com.kaczmarek.bigdata.model.MovieRatingUserAggregateValue
import org.slf4j.LoggerFactory

class MovieRatingUserAggregateReducer
    extends ((MovieRatingUserAggregateValue, MovieRatingUserAggregateValue) => MovieRatingUserAggregateValue) {

    private val logger = LoggerFactory.getLogger(getClass)

    override def apply(
        accumulator: MovieRatingUserAggregateValue,
        value: MovieRatingUserAggregateValue): MovieRatingUserAggregateValue = {
        logger.info(">> {} {}", accumulator, value: Any)
        val result = MovieRatingUserAggregateValue(
            voteCount = accumulator.voteCount + value.voteCount,
            ratingSum = accumulator.ratingSum + value.ratingSum
        )
        logger.info("<< {}", result)
        result
    }
}
