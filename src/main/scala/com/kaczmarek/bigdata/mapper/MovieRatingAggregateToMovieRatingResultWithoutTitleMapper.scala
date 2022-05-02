package com.kaczmarek.bigdata.mapper

import com.kaczmarek.bigdata.model.{MovieRatingAggregateKey, MovieRatingAggregateValue, MovieRatingResultWithoutTitle}

class MovieRatingAggregateToMovieRatingResultWithoutTitleMapper
    extends ((MovieRatingAggregateKey, MovieRatingAggregateValue) => (Int, MovieRatingResultWithoutTitle)) {

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
