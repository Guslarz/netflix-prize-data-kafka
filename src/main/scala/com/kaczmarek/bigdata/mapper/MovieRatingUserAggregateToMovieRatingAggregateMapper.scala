package com.kaczmarek.bigdata.mapper

import com.kaczmarek.bigdata.model.{MovieRatingAggregateKey, MovieRatingAggregateValue, MovieRatingUserAggregateKey, MovieRatingUserAggregateValue}
import org.slf4j.LoggerFactory

class MovieRatingUserAggregateToMovieRatingAggregateMapper
    extends ((MovieRatingUserAggregateKey, MovieRatingUserAggregateValue) =>
        (MovieRatingAggregateKey, MovieRatingAggregateValue)) {

    private val logger = LoggerFactory.getLogger(getClass)

    override def apply(key: MovieRatingUserAggregateKey, value: MovieRatingUserAggregateValue):
    (MovieRatingAggregateKey, MovieRatingAggregateValue) = {
        logger.info(">> {} {}", key, value: Any)
        val result = (
            MovieRatingAggregateKey(
                year = key.year,
                month = key.month,
                movieId = key.movieId
            ),
            MovieRatingAggregateValue(
                voteCount = value.voteCount,
                ratingSum = value.ratingSum,
                uniqueVoterCount = 1
            )
        )
        logger.info("<< {}", result)
        result
    }
}
