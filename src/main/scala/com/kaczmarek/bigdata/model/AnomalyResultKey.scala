package com.kaczmarek.bigdata.model

case class AnomalyResultKey(
    movieId: Int,
    windowStart: String,
    windowEnd: String
)
