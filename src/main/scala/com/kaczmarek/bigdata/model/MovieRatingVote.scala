package com.kaczmarek.bigdata.model

import java.util.Date

case class MovieRatingVote(
    date: Date,
    movieId: Int,
    userId: Int,
    rating: Int
)
