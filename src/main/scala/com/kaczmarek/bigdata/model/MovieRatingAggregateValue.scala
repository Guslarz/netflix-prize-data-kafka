package com.kaczmarek.bigdata.model

case class MovieRatingAggregateValue(
    voteCount: Int,
    ratingSum: Int,
    uniqueVoterCount: Int
)
