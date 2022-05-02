package com.kaczmarek.bigdata.operator.selector

import com.kaczmarek.bigdata.model.{MovieRatingAggregateKey, MovieRatingAggregateValue, MovieRatingUserAggregateKey, MovieRatingUserAggregateValue}

class MovieRatingAggregateSelector
    extends ((MovieRatingUserAggregateKey, MovieRatingUserAggregateValue) =>
        (MovieRatingAggregateKey, MovieRatingAggregateValue)) {

    override def apply(key: MovieRatingUserAggregateKey, value: MovieRatingUserAggregateValue):
    (MovieRatingAggregateKey, MovieRatingAggregateValue) = (
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
}
