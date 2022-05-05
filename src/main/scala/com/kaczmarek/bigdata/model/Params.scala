package com.kaczmarek.bigdata.model

case class Params(
    server: String,
    anomalyWindowDuration: Int,
    anomalyMinimumVoteCount: Int,
    anomalyMinimumRatingAverage: Double
)
