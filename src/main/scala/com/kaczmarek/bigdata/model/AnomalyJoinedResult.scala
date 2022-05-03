package com.kaczmarek.bigdata.model

case class AnomalyJoinedResult(
    windowStart: String,
    windowEnd: String,
    title: String,
    voteCount: Int,
    ratingAverage: Double
)
