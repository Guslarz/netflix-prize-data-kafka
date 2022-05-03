package com.kaczmarek.bigdata.operator.mapper

import com.kaczmarek.bigdata.model.{MovieRatingUserAggregateKey, MovieRatingUserAggregateValue, MovieRatingVote}
import com.kaczmarek.bigdata.util.DateUtils

class MovieRatingVoteToMovieRatingUserAggregateMapper
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
