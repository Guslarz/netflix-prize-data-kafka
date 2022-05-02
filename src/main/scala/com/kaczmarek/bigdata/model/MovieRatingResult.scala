package com.kaczmarek.bigdata.model

case class MovieRatingResult(
    year: Int,
    month: Int,
    title: String,
    voteCount: Int,
    ratingSum: Int,
    uniqueVoterCount: Int
)
