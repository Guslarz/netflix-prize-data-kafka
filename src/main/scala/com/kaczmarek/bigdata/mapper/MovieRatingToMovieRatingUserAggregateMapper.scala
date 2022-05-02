package com.kaczmarek.bigdata.mapper

import com.kaczmarek.bigdata.model.{MovieRatingUserAggregateKey, MovieRatingUserAggregateValue, MovieRatingVote}
import com.kaczmarek.bigdata.util.DateUtils

class MovieRatingToMovieRatingUserAggregateMapper
    extends ((String, MovieRatingVote) => (MovieRatingUserAggregateKey, MovieRatingUserAggregateValue)) {

    override def apply(key: String, vote: MovieRatingVote):
    (MovieRatingUserAggregateKey, MovieRatingUserAggregateValue) = (
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
}
