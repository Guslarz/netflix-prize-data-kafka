package com.kaczmarek.bigdata.mapper

import com.kaczmarek.bigdata.model.MovieRatingResult

class MovieRatingResultToCsvMapper extends (MovieRatingResult => String) {

    override def apply(value: MovieRatingResult): String = Array(
        value.year.toString,
        value.month.toString,
        value.title,
        value.voteCount.toString,
        value.ratingSum.toString,
        value.uniqueVoterCount.toString
    ).mkString(",")
}
