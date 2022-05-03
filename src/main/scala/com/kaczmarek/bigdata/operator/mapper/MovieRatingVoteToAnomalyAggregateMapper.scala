package com.kaczmarek.bigdata.operator.mapper

import com.kaczmarek.bigdata.model.{AnomalyAggregate, MovieRatingVote}

class MovieRatingVoteToAnomalyAggregateMapper extends ((String, MovieRatingVote) => (Int, AnomalyAggregate)) {

    override def apply(key: String, vote: MovieRatingVote): (Int, AnomalyAggregate) = (
        vote.movieId,
        AnomalyAggregate(
            voteCount = 1,
            ratingSum = vote.rating
        )
    )
}
