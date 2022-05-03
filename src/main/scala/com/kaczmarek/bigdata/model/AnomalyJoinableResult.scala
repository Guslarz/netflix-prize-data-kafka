package com.kaczmarek.bigdata.model

case class AnomalyJoinableResult(
    windowStart: String,
    windowEnd: String,
    voteCount: Int,
    ratingAverage: Double
)
