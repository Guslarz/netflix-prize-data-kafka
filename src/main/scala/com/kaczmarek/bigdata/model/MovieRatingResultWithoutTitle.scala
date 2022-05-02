package com.kaczmarek.bigdata.model

case class MovieRatingResultWithoutTitle(
    year: Int,
    month: Int,
    voteCount: Int,
    ratingSum: Int,
    uniqueVoterCount: Int
)
