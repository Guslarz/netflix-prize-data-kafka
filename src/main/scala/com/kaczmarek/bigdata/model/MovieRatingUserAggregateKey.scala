package com.kaczmarek.bigdata.model

case class MovieRatingUserAggregateKey(
    year: Int,
    month: Int,
    movieId: Int,
    userId: Int
)
