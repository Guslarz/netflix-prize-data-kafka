package com.kaczmarek.bigdata.operator.mapper

import com.kaczmarek.bigdata.model.{MovieRatingAggregateKey, MovieRatingAggregateValue, MovieRatingResultWithoutTitle}
import org.slf4j.LoggerFactory

class MovieRatingAggregateToMovieRatingResultWithoutTitleMapper
    extends ((MovieRatingAggregateKey, MovieRatingAggregateValue) => (Int, MovieRatingResultWithoutTitle)) {

    private val logger = LoggerFactory.getLogger(getClass)

    override def apply(key: MovieRatingAggregateKey, value: MovieRatingAggregateValue):
    (Int, MovieRatingResultWithoutTitle) = (
        key.movieId,
        MovieRatingResultWithoutTitle(
            year = key.year,
            month = key.month,
            voteCount = value.voteCount,
            ratingSum = value.ratingSum,
            uniqueVoterCount = value.uniqueVoterCount
        )
    )
}
