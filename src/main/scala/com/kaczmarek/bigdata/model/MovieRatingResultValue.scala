package com.kaczmarek.bigdata.model

case class MovieRatingResultValue(
    title: String,
    voteCount: Int,
    ratingSum: Int,
    uniqueVoterCount: Int
)
