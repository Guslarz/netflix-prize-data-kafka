package com.kaczmarek.bigdata.model

case class Params(
    anomalyWindowDuration: Int,
    anomalyMinimumVoteCount: Int,
    anomalyMinimumRatingAverage: Double
)
