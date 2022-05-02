package com.kaczmarek.bigdata.mapper

import com.kaczmarek.bigdata.model.{MovieRatingUserAggregateKey, MovieRatingUserAggregateValue, MovieRatingVote}
import com.kaczmarek.bigdata.util.DateUtils
import org.slf4j.LoggerFactory

class MovieRatingToMovieRatingUserAggregateMapper
    extends ((String, MovieRatingVote) => (MovieRatingUserAggregateKey, MovieRatingUserAggregateValue)) {

    private val logger = LoggerFactory.getLogger(getClass)

    override def apply(key: String, vote: MovieRatingVote):
    (MovieRatingUserAggregateKey, MovieRatingUserAggregateValue) = {
        logger.info(">> {} {}", key, vote: Any)
        val result = (
            MovieRatingUserAggregateKey(
                year = DateUtils.getYear(vote.date),
                month = DateUtils.getMonth(vote.date),
                movieId = vote.movieId,
                userId = vote.userId
            ),
            MovieRatingUserAggregateValue(
                voteCount = 1,
                ratingSum = vote.rating
            )
        )
        logger.info("<< {}", result)
        result
    }
}
